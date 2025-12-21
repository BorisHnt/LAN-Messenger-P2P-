#!/usr/bin/env python3
"""Tkinter peer-to-peer LAN messenger (single file, no central server)."""

import argparse
import json
import queue
import socket
import threading
import tkinter as tk
import uuid
import time
from dataclasses import dataclass
from tkinter import messagebox, ttk
from typing import Optional


@dataclass
class RoomEntry:
    name: str
    port: int
    private: bool
    creator: str
    code: str = ""
    local: bool = False


class DiscoveryService:
    """Handles room discovery announcements over a fixed UDP broadcast port."""

    DISCOVERY_PORT = 54545

    def __init__(self, peer_id: str, on_rooms_changed) -> None:
        self.peer_id = peer_id
        self.on_rooms_changed = on_rooms_changed
        self.sock: Optional[socket.socket] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        self.rooms: dict[str, RoomEntry] = {}
        self.local_rooms: dict[str, RoomEntry] = {}

    def start(self) -> None:
        if self.running:
            return
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (OSError, AttributeError):
            pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", self.DISCOVERY_PORT))
        sock.settimeout(1.0)
        self.sock = sock
        self.running = True
        self.thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.running = False
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def get_rooms(self) -> list[RoomEntry]:
        with self.lock:
            return list(self.rooms.values())

    def add_local_room(self, room: RoomEntry) -> None:
        key = self._room_key(room)
        room.local = True
        with self.lock:
            self.local_rooms[key] = room
            self.rooms[key] = room
        self._announce(room)
        self._notify()

    def remove_room(self, room: RoomEntry) -> None:
        key = self._room_key(room)
        with self.lock:
            self.local_rooms.pop(key, None)
            removed = self.rooms.pop(key, None)
        if removed:
            self._broadcast(
                {
                    "type": "room_remove",
                    "name": room.name,
                    "port": room.port,
                    "private": room.private,
                    "creator": room.creator,
                }
            )
            self._notify()

    def request_rooms(self) -> None:
        self._broadcast({"type": "room_request", "from": self.peer_id})

    def _announce(self, room: RoomEntry) -> None:
        self._broadcast(
            {
                "type": "room_announce",
                "name": room.name,
                "port": room.port,
                "private": room.private,
                "creator": room.creator,
            }
        )

    def _broadcast(self, payload: dict) -> None:
        if self.sock is None:
            return
        try:
            data = json.dumps(payload).encode("utf-8")
            self.sock.sendto(data, ("<broadcast>", self.DISCOVERY_PORT))
        except OSError:
            pass

    def _recv_loop(self) -> None:
        assert self.sock is not None
        while self.running:
            try:
                data, _addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                payload = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            msg_type = payload.get("type")
            if msg_type == "room_request":
                self._handle_request()
            elif msg_type == "room_announce":
                self._handle_announce(payload)
            elif msg_type == "room_remove":
                self._handle_remove(payload)

        self.running = False
        self.sock = None

    def _handle_request(self) -> None:
        with self.lock:
            rooms = list(self.local_rooms.values())
        for room in rooms:
            self._announce(room)

    def _handle_announce(self, payload: dict) -> None:
        try:
            port = int(payload.get("port"))
        except (TypeError, ValueError):
            return
        name = str(payload.get("name", ""))
        private = bool(payload.get("private"))
        creator = str(payload.get("creator", ""))
        if not name or not creator:
            return
        room = RoomEntry(name=name, port=port, private=private, creator=creator, code="")
        key = self._room_key(room)
        with self.lock:
            self.rooms[key] = room
        self._notify()

    def _handle_remove(self, payload: dict) -> None:
        try:
            port = int(payload.get("port"))
        except (TypeError, ValueError):
            return
        name = str(payload.get("name", ""))
        creator = str(payload.get("creator", ""))
        private = bool(payload.get("private"))
        room = RoomEntry(name=name, port=port, private=private, creator=creator)
        key = self._room_key(room)
        with self.lock:
            removed = self.rooms.pop(key, None)
            self.local_rooms.pop(key, None)
        if removed:
            self._notify()

    def _room_key(self, room: RoomEntry) -> str:
        return f"{room.name}|{room.port}|{room.creator}"

    def _notify(self) -> None:
        if self.on_rooms_changed:
            self.on_rooms_changed()


class BroadcastPeer:
    """Sends/receives chat messages over UDP broadcast on a shared port."""

    def __init__(self, on_message, on_presence=None) -> None:
        self.on_message = on_message
        self.on_presence = on_presence
        self.sock: Optional[socket.socket] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.presence_thread: Optional[threading.Thread] = None
        self.peer_id = uuid.uuid4().hex[:8]
        self.name = ""
        self.port = 0
        self.room = "public"
        self.code = ""

    def start(self, name: str, port: int, room: str, code: str) -> None:
        if self.running:
            return
        self.name = name or "Anonymous"
        self.port = port
        self.room = room or "public"
        self.code = code.strip()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (OSError, AttributeError):
            pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", port))
        sock.settimeout(1.0)
        self.sock = sock
        self.running = True
        self.thread = threading.Thread(target=self._recv_loop, daemon=True)
        self.thread.start()
        self._send_system("joined the chat")
        self.presence_thread = threading.Thread(target=self._presence_loop, daemon=True)
        self.presence_thread.start()

    def send_chat(self, text: str) -> None:
        self._send({"type": "chat", "text": text})

    def stop(self, announce: bool = True) -> None:
        if not self.running:
            return
        if announce:
            self._send_system("left the chat")
        self.running = False
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _recv_loop(self) -> None:
        assert self.sock is not None
        while self.running:
            try:
                data, _addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            try:
                payload = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            if payload.get("id") == self.peer_id:
                continue

            msg_type = payload.get("type")
            name = payload.get("name", "Unknown")
            text = payload.get("text", "")
            room = payload.get("room", "public")
            code = payload.get("code", "")

            if room != self.room or code != self.code:
                continue

            if msg_type == "presence" and self.on_presence:
                self.on_presence(payload.get("id", ""), name)
                continue

            if msg_type == "chat":
                display = f"{name}: {text}"
            elif msg_type == "system":
                display = f"* {name} {text}"
            else:
                continue
            self.on_message(display)

        self.running = False
        self.sock = None

    def _send(self, payload: dict) -> None:
        if self.sock is None:
            return
        payload.update(
            {
                "id": self.peer_id,
                "name": self.name,
                "room": self.room,
                "code": self.code,
            }
        )
        try:
            data = json.dumps(payload).encode("utf-8")
        except (TypeError, ValueError):
            return
        try:
            self.sock.sendto(data, ("<broadcast>", self.port))
        except OSError:
            pass

    def _send_system(self, text: str) -> None:
        self._send({"type": "system", "text": text})

    def _presence_loop(self) -> None:
        while self.running:
            if self.on_presence:
                self.on_presence(self.peer_id, self.name)
            self._send({"type": "presence"})
            time.sleep(5)


class MessengerApp:
    """Tkinter UI that chats with all peers listening on the same UDP port."""

    def __init__(
        self,
        root: tk.Tk,
        default_port: int,
        default_name: str,
        default_room: str = "public",
        default_code: str = "",
    ):
        self.root = root
        self.root.title("LAN Messenger (P2P)")
        self.messages = queue.Queue()
        self.peer = BroadcastPeer(on_message=self.messages.put, on_presence=self._on_presence)
        self.connected = False
        self._create_win: Optional[tk.Toplevel] = None
        self.current_room_key: Optional[str] = None

        self.port_var = tk.StringVar(value=str(default_port))
        self.room_var = tk.StringVar(value=default_room)
        self.code_var = tk.StringVar(value=default_code)
        self.name_var = tk.StringVar(value=default_name)
        self.msg_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Disconnected")

        self.discovery = DiscoveryService(self.peer.peer_id, self._on_rooms_updated)
        self.discovery.start()

        self.rooms: list[RoomEntry] = []
        self.presence: dict[str, dict[str, dict]] = {}
        self._build_ui()
        self._on_rooms_updated()
        self.discovery.request_rooms()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._poll_messages)

    def _build_ui(self) -> None:
        padding = {"padx": 8, "pady": 4}

        container = ttk.Frame(self.root)
        container.pack(fill="both", expand=True)

        # Left panel for discovered rooms (port/room/code presets)
        left_frame = ttk.Frame(container, width=280)
        left_frame.pack(side="left", fill="y")
        left_frame.pack_propagate(False)

        ttk.Label(left_frame, text="Rooms (port / room)").pack(anchor="w", padx=8, pady=(8, 4))

        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill="both", expand=True, padx=8)

        self.room_list = tk.Listbox(list_frame, height=16)
        self.room_list.pack(side="left", fill="both", expand=True)
        bm_scroll = ttk.Scrollbar(list_frame, command=self.room_list.yview)
        bm_scroll.pack(side="right", fill="y")
        self.room_list.configure(yscrollcommand=bm_scroll.set)
        self.room_list.bind("<<ListboxSelect>>", self._on_room_select)

        bm_btns = ttk.Frame(left_frame)
        bm_btns.pack(fill="x", padx=8, pady=8)
        ttk.Button(bm_btns, text="Sync", command=self._sync_rooms).pack(fill="x", pady=(0, 4))
        ttk.Button(bm_btns, text="Add", command=self._open_create_room_window).pack(
            fill="x", pady=(0, 4)
        )
        ttk.Button(bm_btns, text="Delete", command=self._delete_room).pack(fill="x")

        ttk.Label(left_frame, text="Participants").pack(anchor="w", padx=8, pady=(4, 4))
        self.participants_list = tk.Listbox(left_frame, height=8)
        self.participants_list.pack(fill="both", expand=False, padx=8, pady=(0, 8))

        # Right side: controls + chat
        right_frame = ttk.Frame(container)
        right_frame.pack(side="left", fill="both", expand=True)

        top_frame = ttk.Frame(right_frame)
        top_frame.pack(fill="x", **padding)

        ttk.Label(top_frame, text="Port").grid(row=0, column=0, sticky="w")
        self.port_entry = ttk.Entry(top_frame, textvariable=self.port_var, width=8)
        self.port_entry.grid(row=1, column=0, sticky="we", padx=(0, 6))

        ttk.Label(top_frame, text="Room").grid(row=0, column=1, sticky="w")
        self.room_entry = ttk.Entry(top_frame, textvariable=self.room_var, width=14)
        self.room_entry.grid(row=1, column=1, sticky="we", padx=(0, 6))

        ttk.Label(top_frame, text="Code (optional)").grid(row=0, column=2, sticky="w")
        self.code_entry = ttk.Entry(top_frame, textvariable=self.code_var, width=12)
        self.code_entry.grid(row=1, column=2, sticky="we", padx=(0, 6))

        ttk.Label(top_frame, text="Nickname").grid(row=0, column=3, sticky="w")
        self.name_entry = ttk.Entry(top_frame, textvariable=self.name_var, width=14)
        self.name_entry.grid(row=1, column=3, sticky="we", padx=(0, 6))

        self.connect_button = ttk.Button(top_frame, text="Connect", command=self._connect)
        self.connect_button.grid(row=1, column=4, sticky="we", padx=(0, 6))

        self.disconnect_button = ttk.Button(
            top_frame, text="Disconnect", command=self._disconnect, state="disabled"
        )
        self.disconnect_button.grid(row=1, column=5, sticky="we")

        top_frame.columnconfigure(0, weight=1)
        top_frame.columnconfigure(1, weight=2)
        top_frame.columnconfigure(2, weight=2)
        top_frame.columnconfigure(3, weight=2)
        top_frame.columnconfigure(4, weight=1)
        top_frame.columnconfigure(5, weight=1)

        middle_frame = ttk.Frame(right_frame)
        middle_frame.pack(fill="both", expand=True, **padding)

        self.text_area = tk.Text(
            middle_frame, height=18, state="disabled", wrap="word", bg="#1e1e1e", fg="#e6e6e6"
        )
        scrollbar = ttk.Scrollbar(middle_frame, command=self.text_area.yview)
        self.text_area.configure(yscrollcommand=scrollbar.set)
        self.text_area.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        bottom_frame = ttk.Frame(right_frame)
        bottom_frame.pack(fill="x", **padding)

        self.message_entry = ttk.Entry(bottom_frame, textvariable=self.msg_var)
        self.message_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self.message_entry.bind("<Return>", self._send_event)

        self.send_button = ttk.Button(bottom_frame, text="Send", command=self._send_message)
        self.send_button.pack(side="right")

        status_frame = ttk.Frame(right_frame)
        status_frame.pack(fill="x", **padding)
        ttk.Label(status_frame, textvariable=self.status_var).pack(side="left")

        self._update_ui_state()

    def _on_rooms_updated(self) -> None:
        self.rooms = sorted(
            self.discovery.get_rooms(), key=lambda r: (r.port, r.name.lower(), r.code)
        )
        self._refresh_room_list()

    def _refresh_room_list(self) -> None:
        self.room_list.delete(0, "end")
        for room in self.rooms:
            privacy = "private" if room.private else "public"
            suffix = " [mine]" if room.creator == self.peer.peer_id else ""
            label = f"{room.name} @ {room.port} ({privacy}){suffix}"
            self.room_list.insert("end", label)

    def _on_room_select(self, _event=None) -> None:
        selection = self.room_list.curselection()
        if not selection:
            return
        idx = selection[0]
        if idx >= len(self.rooms):
            return
        room = self.rooms[idx]
        self.port_var.set(str(room.port))
        self.room_var.set(room.name)
        if room.local:
            self.code_var.set(room.code)
        else:
            self.code_var.set("")
        self._refresh_participants_display()

    def _sync_rooms(self) -> None:
        self.discovery.request_rooms()

    def _open_create_room_window(self) -> None:
        if hasattr(self, "_create_win") and self._create_win is not None:
            self._create_win.lift()
            return
        win = tk.Toplevel(self.root)
        win.title("Create Room")
        self._create_win = win

        padding = {"padx": 10, "pady": 6}
        ttk.Label(win, text="Port").grid(row=0, column=0, sticky="w", **padding)
        port_entry = ttk.Entry(win)
        port_entry.insert(0, self.port_var.get())
        port_entry.grid(row=0, column=1, sticky="we", **padding)

        ttk.Label(win, text="Room name").grid(row=1, column=0, sticky="w", **padding)
        room_entry = ttk.Entry(win)
        room_entry.insert(0, self.room_var.get())
        room_entry.grid(row=1, column=1, sticky="we", **padding)

        ttk.Label(win, text="Code (optional)").grid(row=2, column=0, sticky="w", **padding)
        code_entry = ttk.Entry(win)
        code_entry.insert(0, self.code_var.get())
        code_entry.grid(row=2, column=1, sticky="we", **padding)

        create_btn = ttk.Button(
            win,
            text="Create the Room",
            command=lambda: self._create_room(win, port_entry, room_entry, code_entry),
        )
        create_btn.grid(row=3, column=0, columnspan=2, sticky="we", padx=10, pady=(4, 10))

        win.columnconfigure(1, weight=1)

        def on_close() -> None:
            self._create_win = None
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", on_close)

    def _create_room(self, win: tk.Toplevel, port_entry: ttk.Entry, room_entry: ttk.Entry, code_entry: ttk.Entry) -> None:
        room_name = room_entry.get().strip() or "public"
        code = code_entry.get().strip()
        try:
            port = int(port_entry.get().strip())
        except ValueError:
            messagebox.showerror("Invalid port", "Port must be a number.")
            return

        # Check if port is free locally (UDP bind test).
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            test_sock.bind(("", port))
        except OSError:
            messagebox.showerror("Port unavailable", f"Port {port} cannot be bound on this host.")
            return
        finally:
            try:
                test_sock.close()
            except OSError:
                pass

        room = RoomEntry(
            name=room_name,
            port=port,
            code=code,
            private=bool(code),
            creator=self.peer.peer_id,
            local=True,
        )
        self.discovery.add_local_room(room)
        self._on_rooms_updated()
        self.port_var.set(str(port))
        self.room_var.set(room_name)
        self.code_var.set(code)
        self._create_win = None
        win.destroy()

    def _delete_room(self) -> None:
        selection = self.room_list.curselection()
        if not selection:
            messagebox.showwarning("No selection", "Select a room to delete.")
            return
        idx = selection[0]
        if idx >= len(self.rooms):
            return
        room = self.rooms[idx]
        if room.creator != self.peer.peer_id:
            messagebox.showwarning("Cannot delete", "Only the creator can delete this room.")
            return
        self.discovery.remove_room(room)
        self._on_rooms_updated()

    def _on_presence(self, peer_id: str, name: str) -> None:
        if not self.connected or not self.current_room_key:
            return
        peers = self.presence.setdefault(self.current_room_key, {})
        peers[peer_id] = {"name": name, "last": time.time()}
        self._refresh_participants_display()

    def _refresh_participants_display(self) -> None:
        if self.connected and self.current_room_key:
            key = self.current_room_key
        else:
            # Use currently selected fields to try matching known presence (requires correct code).
            try:
                port = int(self.port_var.get().strip())
            except ValueError:
                port = -1
            key = self._room_key(port, self.room_var.get().strip() or "public", self.code_var.get().strip())
        self.participants_list.delete(0, "end")
        if not key or key not in self.presence:
            return
        peers = self.presence.get(key, {})
        # Sort by name then peer_id
        items = sorted(peers.items(), key=lambda item: (item[1]["name"].lower(), item[0]))
        for _, data in items:
            self.participants_list.insert("end", data["name"])

    def _room_key(self, port: int, room: str, code: str) -> str:
        return f"{port}|{room}|{code}"

    def _connect(self) -> None:
        name = self.name_var.get().strip() or "Anonymous"
        room = self.room_var.get().strip() or "public"
        code = self.code_var.get().strip()
        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            messagebox.showerror("Invalid port", "Port must be a number.")
            return

        try:
            self.peer.start(name, port, room, code)
        except OSError as exc:
            messagebox.showerror("Could not start", f"Failed to bind port: {exc}")
            return

        self.connected = True
        priv = "private" if code else "public"
        self.status_var.set(f"UDP {port} | room {room} ({priv}) | {name}")
        self.current_room_key = self._room_key(port, room, code)
        # Seed presence with self
        peers = self.presence.setdefault(self.current_room_key, {})
        peers[self.peer.peer_id] = {"name": name, "last": time.time()}
        self._update_ui_state()
        self._append_message("Connected. Peers on this port/room will see your messages.")
        self.message_entry.focus_set()
        self._refresh_participants_display()

    def _disconnect(self) -> None:
        if not self.connected:
            return
        self.peer.stop(announce=True)
        self.connected = False
        self.status_var.set("Disconnected")
        self._update_ui_state()
        self._append_message("Disconnected.")
        self.current_room_key = None
        self.participants_list.delete(0, "end")

    def _send_event(self, event: tk.Event) -> str | None:
        self._send_message()
        return "break"

    def _send_message(self) -> None:
        if not self.connected:
            messagebox.showwarning("Not connected", "Click Connect first.")
            return
        message = self.msg_var.get().strip()
        if not message:
            return
        self.peer.send_chat(message)
        self._append_message(f"Me: {message}")
        self.msg_var.set("")

    def _poll_messages(self) -> None:
        while True:
            try:
                msg = self.messages.get_nowait()
            except queue.Empty:
                break
            self._append_message(msg)
        self._cleanup_presence()
        self.root.after(100, self._poll_messages)

    def _append_message(self, message: str) -> None:
        self.text_area.configure(state="normal")
        self.text_area.insert("end", message + "\n")
        self.text_area.configure(state="disabled")
        self.text_area.see("end")

    def _cleanup_presence(self) -> None:
        if not self.presence:
            return
        now = time.time()
        changed = False
        for room_key in list(self.presence.keys()):
            peers = self.presence.get(room_key, {})
            for pid in list(peers.keys()):
                if now - peers[pid]["last"] > 20:
                    peers.pop(pid, None)
                    changed = True
            if not peers:
                self.presence.pop(room_key, None)
                changed = True
        if changed:
            self._refresh_participants_display()

    def _update_ui_state(self) -> None:
        entries_state = "disabled" if self.connected else "normal"
        self.port_entry.configure(state=entries_state)
        self.room_entry.configure(state=entries_state)
        self.code_entry.configure(state=entries_state)
        self.name_entry.configure(state=entries_state)
        self.connect_button.configure(state="disabled" if self.connected else "normal")
        self.disconnect_button.configure(state="normal" if self.connected else "disabled")
        self.send_button.configure(state="normal" if self.connected else "disabled")
        self.message_entry.configure(state="normal" if self.connected else "disabled")

    def _on_close(self) -> None:
        self.peer.stop(announce=True)
        self.connected = False
        self.discovery.stop()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tkinter LAN messenger (peer-to-peer)")
    parser.add_argument("--port", type=int, default=5050, help="UDP port shared by all peers")
    parser.add_argument("--room", default="public", help="Room/channel name")
    parser.add_argument("--code", default="", help="Private code (must match to receive)")
    parser.add_argument("--name", default="", help="Display name")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = tk.Tk()
    app = MessengerApp(root, args.port, args.name, args.room, args.code)
    root.mainloop()

if __name__ == "__main__":
    main()
