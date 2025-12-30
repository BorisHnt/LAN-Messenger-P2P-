#!/usr/bin/env python3
"""Tkinter peer-to-peer LAN messenger with multi-room support and per-room buffers."""

import argparse
import queue
import socket
import tkinter as tk
import uuid
import time
from collections import deque
from dataclasses import dataclass
from tkinter import messagebox, ttk
from typing import Optional
from p2p_server import BroadcastPeer, DiscoveryService, RoomEntry

AUTO_ROOM_PORT = 4242
AUTO_ROOM_NAME = "42 Global"
AUTO_ROOM_CODE = ""


@dataclass
class SessionState:
    key: tuple[int, str, str]
    port: int
    room: str
    code: str
    creator: str
    peer: "BroadcastPeer"
    buffer: list[str]
    presence: dict[str, dict]
    typing_states: dict[str, float]
    outgoing_times: deque
    send_penalty_until: float
    last_typing_sent: float
    last_typing_activity: float
    is_typing: bool
    last_room_announce: float
    connected: bool = False


class MessengerApp:
    """Tkinter UI that chats with multiple rooms; each room has its own session and buffer."""

    def __init__(
        self,
        root: tk.Tk,
        default_port: int = AUTO_ROOM_PORT,
        default_name: str = "",
        default_room: str = AUTO_ROOM_NAME,
        default_code: str = AUTO_ROOM_CODE,
    ):
        self.root = root
        self.root.title("LAN Messenger (P2P)")
        self.messages = queue.Queue()
        self.peer_id = uuid.uuid4().hex[:8]
        self._create_win: Optional[tk.Toplevel] = None

        self.port_var = tk.StringVar(value=str(default_port))
        self.room_var = tk.StringVar(value=default_room)
        self.code_var = tk.StringVar(value=default_code)
        self.name_var = tk.StringVar(value=default_name)
        self.msg_var = tk.StringVar()
        self.status_var = tk.StringVar(value="Disconnected")
        self.typing_var = tk.StringVar(value="")

        self.discovery = DiscoveryService(self.peer_id, self._on_rooms_updated)
        self.discovery.start()

        self.rooms: list[RoomEntry] = []
        self.sessions: dict[tuple[int, str, str], SessionState] = {}
        self.current_session_key: Optional[tuple[int, str, str]] = None
        self.connected_ids: set[tuple[int, str]] = set()

        self._build_ui()
        self._maybe_create_default_room()
        self._on_rooms_updated()
        self.discovery.request_rooms()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._poll_messages)

    # ---------------- UI -----------------
    def _build_ui(self) -> None:
        padding = {"padx": 8, "pady": 4}

        container = ttk.Frame(self.root)
        container.pack(fill="both", expand=True)

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
        ttk.Button(bm_btns, text="Create Room", command=self._open_create_room_window).pack(
            fill="x", pady=(0, 4)
        )
        ttk.Button(bm_btns, text="Delete Room", command=self._delete_room).pack(fill="x")

        ttk.Label(left_frame, text="Participants").pack(anchor="w", padx=8, pady=(4, 4))
        self.participants_list = tk.Listbox(left_frame, height=8)
        self.participants_list.pack(fill="both", expand=False, padx=8, pady=(0, 8))

        right_frame = ttk.Frame(container)
        right_frame.pack(side="left", fill="both", expand=True)

        top_frame = ttk.Frame(right_frame)
        top_frame.pack(fill="x", **padding)

        ttk.Label(top_frame, text="Port").grid(row=0, column=0, sticky="w")
        self.port_label_var = tk.StringVar(value=self.port_var.get())
        ttk.Label(top_frame, textvariable=self.port_label_var).grid(row=1, column=0, sticky="w", padx=(0, 6))

        ttk.Label(top_frame, text="Room").grid(row=0, column=1, sticky="w")
        self.room_label_var = tk.StringVar(value=self.room_var.get())
        ttk.Label(top_frame, textvariable=self.room_label_var).grid(row=1, column=1, sticky="w", padx=(0, 6))

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

        typing_frame = ttk.Frame(right_frame)
        typing_frame.pack(fill="x", **padding)
        self.typing_label = ttk.Label(typing_frame, textvariable=self.typing_var, foreground="#a0a0a0")
        self.typing_label.pack(side="left", anchor="w")

        bottom_frame = ttk.Frame(right_frame)
        bottom_frame.pack(fill="x", **padding)

        self.message_entry = ttk.Entry(bottom_frame, textvariable=self.msg_var)
        self.message_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self.message_entry.bind("<Return>", self._send_event)
        self.message_entry.bind("<KeyRelease>", self._typing_event)

        self.send_button = ttk.Button(bottom_frame, text="Send", command=self._send_message)
        self.send_button.pack(side="right")

        status_frame = ttk.Frame(right_frame)
        status_frame.pack(fill="x", **padding)
        ttk.Label(status_frame, textvariable=self.status_var).pack(side="left")

        self._update_ui_state()

    # -------- Room list & selection ---------
    def _on_rooms_updated(self) -> None:
        self.rooms = sorted(
            self.discovery.get_rooms(), key=lambda r: (r.port, r.name.lower(), r.creator)
        )
        self._refresh_room_list()

    def _refresh_room_list(self) -> None:
        self.room_list.delete(0, "end")
        active_id = self.current_session_key[:2] if self.current_session_key else None
        connected_ids = set(self.connected_ids)
        for idx, room in enumerate(self.rooms):
            privacy = "private" if room.private else "public"
            suffix = " [mine]" if room.creator == self.peer_id else ""
            label = f"{room.name} @ {room.port} ({privacy}){suffix}"
            self.room_list.insert("end", label)
            rid = (room.port, room.name)
            if active_id and rid == active_id:
                self.room_list.itemconfig(idx, fg="#0b3d91")  # active
            elif rid in connected_ids:
                self.room_list.itemconfig(idx, fg="#228b22")  # connected
            else:
                self.room_list.itemconfig(idx, fg="black")

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
        self.port_label_var.set(str(room.port))
        self.room_label_var.set(room.name)
        # Keep code as entered; we don't reveal codes
        self._refresh_participants_display()
        self._refresh_room_list()
        # If already connected to this room (any code), switch view to that session
        for key in self.sessions:
            if (key[0], key[1]) == (room.port, room.name):
                self._set_active_session(key)
                return

    # ------------- Session helpers ---------------
    def _session_for_key(self, key: tuple[int, str, str]) -> Optional[SessionState]:
        return self.sessions.get(key)

    def _set_active_session(self, key: tuple[int, str, str]) -> None:
        self.current_session_key = key
        session = self.sessions.get(key)
        if session:
            self.port_var.set(str(session.port))
            self.room_var.set(session.room)
            self.port_label_var.set(str(session.port))
            self.room_label_var.set(session.room)
            self._load_buffer(session)
            self._refresh_participants_display()
            self._refresh_typing_display(session)
            self.status_var.set(f"UDP {session.port} | room {session.room}")
        self._update_ui_state()
        self._refresh_room_list()

    def _load_buffer(self, session: SessionState) -> None:
        self.text_area.configure(state="normal")
        self.text_area.delete("1.0", "end")
        for line in session.buffer:
            self.text_area.insert("end", line + "\n")
        self.text_area.configure(state="disabled")
        self.text_area.see("end")

    def _append_to_session(self, key: tuple[int, str, str], message: str) -> None:
        session = self.sessions.get(key)
        if not session:
            return
        session.buffer.append(message)
        if key == self.current_session_key:
            self.text_area.configure(state="normal")
            self.text_area.insert("end", message + "\n")
            self.text_area.configure(state="disabled")
            self.text_area.see("end")

    # ------------- Networking callbacks -------------
    def _handle_peer_message(self, key: tuple[int, str, str], message: str) -> None:
        self.messages.put(("msg", key, message))

    def _handle_peer_presence(self, key: tuple[int, str, str], peer_id: str, name: str) -> None:
        self.messages.put(("presence", key, peer_id, name))

    def _handle_peer_typing(self, key: tuple[int, str, str], peer_id: str, name: str, active: bool) -> None:
        self.messages.put(("typing", key, peer_id, name, active))

    # ------------- Connect / Disconnect -------------
    def _connect(self) -> None:
        name = self.name_var.get().strip() or "Anonymous"
        room = self.room_var.get().strip()
        if not room:
            messagebox.showwarning("No room", "Select a room or create one first.")
            return
        code = self.code_var.get().strip()
        try:
            port = int(self.port_var.get().strip())
        except ValueError:
            messagebox.showerror("Invalid port", "Port must be a number.")
            return

        key = (port, room, code)
        creator = self._resolve_room_creator(port, room)
        now = time.time()
        session = self.sessions.get(key)
        if session and session.connected:
            self._set_active_session(key)
            return

        peer = BroadcastPeer(
            peer_id=self.peer_id,
            session_key=key,
            on_message=self._handle_peer_message,
            on_presence=self._handle_peer_presence,
            on_typing=self._handle_peer_typing,
        )
        try:
            peer.start(name, port, room, code)
        except OSError as exc:
            messagebox.showerror("Could not start", f"Failed to bind port: {exc}")
            return

        session = SessionState(
            key=key,
            port=port,
            room=room,
            code=code,
            creator=creator,
            peer=peer,
            buffer=[],
            presence={},
            typing_states={},
            outgoing_times=deque(),
            send_penalty_until=0.0,
            last_typing_sent=0.0,
            last_typing_activity=0.0,
            is_typing=False,
            last_room_announce=time.time(),
            connected=True,
        )
        # seed presence with self
        session.presence[self.peer_id] = {"name": name, "last": now, "first": now}
        self.sessions[key] = session
        self.connected_ids.add((port, room))
        self._set_active_session(key)
        self._broadcast_room_advertisement(session)
        self._append_to_session(key, "Connected. Peers on this port/room will see your messages.")
        self.message_entry.focus_set()

    def _resolve_room_creator(self, port: int, room: str) -> str:
        for entry in self.rooms:
            if entry.port == port and entry.name == room:
                return entry.creator
        return self.peer_id

    def _stop_session(self, key: tuple[int, str, str], announce: bool = True) -> None:
        session = self.sessions.pop(key, None)
        if session:
            session.peer.send_typing(False)
            session.peer.stop(announce=announce)
            self.connected_ids.discard((session.port, session.room))
        if self.current_session_key == key:
            self.current_session_key = None
            self.status_var.set("Disconnected")
            self._update_ui_state()
            self.participants_list.delete(0, "end")
            self.typing_var.set("")
            self.text_area.configure(state="normal")
            self.text_area.delete("1.0", "end")
            self.text_area.configure(state="disabled")
        else:
            self._update_ui_state()
        self._refresh_room_list()

    def _disconnect(self) -> None:
        if not self.current_session_key:
            messagebox.showwarning("Not connected", "Select a connected room first.")
            return
        key = self.current_session_key
        self._stop_session(key)

    # ------------- Message sending -------------
    def _send_event(self, event: tk.Event) -> str | None:
        self._send_message()
        return "break"

    def _send_message(self) -> None:
        if not self.current_session_key:
            messagebox.showwarning("Not connected", "Connect to a room first.")
            return
        session = self.sessions.get(self.current_session_key)
        if not session or not session.connected:
            messagebox.showwarning("Not connected", "Connect to a room first.")
            return
        now = time.time()
        if now < session.send_penalty_until:
            wait = int(session.send_penalty_until - now) + 1
            self._append_to_session(session.key, f"Flood control: wait {wait}s before sending.")
            return
        message = self.msg_var.get().strip()
        if not message:
            return
        self._prune_outgoing(session, now)
        if len(session.outgoing_times) >= session.peer.flood_limit_count:
            session.send_penalty_until = now + session.peer.flood_penalty_seconds
            self._append_to_session(session.key, "Flood control: 10s cooldown applied.")
            return
        session.outgoing_times.append(now)
        session.peer.send_chat(message)
        session.is_typing = False
        session.peer.send_typing(False)
        self._append_to_session(session.key, f"Me: {message}")
        self.msg_var.set("")
        session.last_typing_activity = 0.0

    def _prune_outgoing(self, session: SessionState, now: float) -> None:
        while session.outgoing_times and now - session.outgoing_times[0] > session.peer.flood_limit_window:
            session.outgoing_times.popleft()

    # ------------- Typing UI -------------
    def _typing_event(self, _event=None) -> None:
        if not self.current_session_key:
            return
        session = self.sessions.get(self.current_session_key)
        if not session:
            return
        session.is_typing = True
        session.last_typing_activity = time.time()
        if time.time() - session.last_typing_sent > 1.5:
            session.peer.send_typing(True)
            session.last_typing_sent = time.time()

    def _refresh_typing_display(self, session: Optional[SessionState] = None) -> None:
        session = session or (self.sessions.get(self.current_session_key) if self.current_session_key else None)
        if not session:
            self.typing_var.set("")
            return
        now = time.time()
        stale = [pid for pid, ts in session.typing_states.items() if now - ts > 5]
        for pid in stale:
            session.typing_states.pop(pid, None)
        if not session.typing_states:
            self.typing_var.set("")
            return
        peers = session.presence
        names = []
        for pid in session.typing_states:
            if pid == self.peer_id:
                names.append(self.name_var.get().strip() or "Me")
            elif pid in peers:
                names.append(peers[pid]["name"])
        names = [n for n in names if n]
        if not names:
            self.typing_var.set("")
            return
        if len(names) == 1:
            self.typing_var.set(f"[{names[0]}] is typing...")
        else:
            listed = ", ".join(f"[{n}]" for n in names)
            self.typing_var.set(f"{listed} are typing...")

    # ------------- Presence & typing callbacks -------------
    def _on_presence(self, key: tuple[int, str, str], peer_id: str, name: str) -> None:
        session = self.sessions.get(key)
        if not session:
            return
        now = time.time()
        if peer_id not in session.presence:
            session.presence[peer_id] = {"name": name, "last": now, "first": now}
        else:
            session.presence[peer_id]["name"] = name
            session.presence[peer_id]["last"] = now
            session.presence[peer_id].setdefault("first", now)
        if key == self.current_session_key:
            self._refresh_participants_display()

    def _on_typing(self, key: tuple[int, str, str], peer_id: str, name: str, active: bool) -> None:
        session = self.sessions.get(key)
        if not session:
            return
        if active:
            session.typing_states[peer_id] = time.time()
        else:
            session.typing_states.pop(peer_id, None)
        if key == self.current_session_key:
            self._refresh_typing_display(session)

    def _refresh_participants_display(self) -> None:
        self.participants_list.delete(0, "end")
        if not self.current_session_key:
            return
        session = self.sessions.get(self.current_session_key)
        if not session:
            return
        now = time.time()
        stale = [pid for pid, data in session.presence.items() if now - data.get("last", 0) > 20]
        for pid in stale:
            session.presence.pop(pid, None)
        peers = session.presence
        items = sorted(peers.items(), key=lambda item: (item[1]["name"].lower(), item[0]))
        for _, data in items:
            self.participants_list.insert("end", data["name"])

    def _session_admin(self, session: SessionState) -> Optional[str]:
        if not session.presence:
            return None
        candidates = []
        for pid, data in session.presence.items():
            first_seen = data.get("first") or data.get("last") or 0.0
            candidates.append((first_seen, pid))
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[0][1] if candidates else None

    # ------------- Poll loop -------------
    def _poll_messages(self) -> None:
        while True:
            try:
                evt = self.messages.get_nowait()
            except queue.Empty:
                break
            kind = evt[0]
            if kind == "msg":
                _, key, msg = evt
                self._append_to_session(key, msg)
            elif kind == "presence":
                _, key, pid, name = evt
                self._on_presence(key, pid, name)
            elif kind == "typing":
                _, key, pid, name, active = evt
                self._on_typing(key, pid, name, active)
        # house-keeping
        self._cleanup_presence_typing()
        now = time.time()
        if self.current_session_key:
            session = self.sessions.get(self.current_session_key)
            if session:
                if session.is_typing and now - session.last_typing_activity > 3:
                    session.peer.send_typing(False)
                    session.is_typing = False
                self._refresh_typing_display(session)
        # re-announce connected rooms periodically
        for session in list(self.sessions.values()):
            if not session.connected:
                continue
            if now - session.last_room_announce > 8:
                self._broadcast_room_advertisement(session)
                session.last_room_announce = now
        self.root.after(100, self._poll_messages)

    # ------------- Room advertisement -------------
    def _broadcast_room_advertisement(self, session: SessionState) -> None:
        room_entry = RoomEntry(
            name=session.room,
            port=session.port,
            private=bool(session.code),
            creator=session.creator,
        )
        self.discovery.announce_room(room_entry)

    def _port_is_available(self, port: int) -> bool:
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            test_sock.bind(("", port))
            return True
        except OSError:
            return False
        finally:
            try:
                test_sock.close()
            except OSError:
                pass

    def _maybe_create_default_room(self) -> None:
        """Create and announce the default room if the port is free."""
        if not self._port_is_available(AUTO_ROOM_PORT):
            print(f"[auto-room] Port {AUTO_ROOM_PORT} unavailable; default room not created.")
            return
        room = RoomEntry(
            name=AUTO_ROOM_NAME,
            port=AUTO_ROOM_PORT,
            code=AUTO_ROOM_CODE,
            private=False,
            creator=self.peer_id,
            local=True,
        )
        self.discovery.add_local_room(room)

    # ------------- Create/Delete room -------------
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

        if not self._port_is_available(port):
            messagebox.showerror("Port unavailable", f"Port {port} cannot be bound on this host.")
            return

        room = RoomEntry(
            name=room_name,
            port=port,
            code=code,
            private=bool(code),
            creator=self.peer_id,
            local=True,
        )
        self.discovery.add_local_room(room)
        self._on_rooms_updated()
        self.port_var.set(str(port))
        self.room_var.set(room_name)
        self.port_label_var.set(str(port))
        self.room_label_var.set(room_name)
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
        room_sessions = [s for s in self.sessions.values() if s.port == room.port and s.room == room.name]
        session = room_sessions[0] if room_sessions else None
        is_creator = room.creator == self.peer_id
        is_admin = session and self._session_admin(session) == self.peer_id
        if not (is_creator or is_admin):
            messagebox.showwarning(
                "Cannot delete",
                "Only the creator or the current admin (oldest participant) can delete this room.",
            )
            return
        self.discovery.remove_room(room)
        for sess in room_sessions:
            self._stop_session(sess.key)
        self._on_rooms_updated()

    # ------------- Cleanup / UI state -------------
    def _cleanup_presence_typing(self) -> None:
        now = time.time()
        for session in self.sessions.values():
            stale = [pid for pid, data in session.presence.items() if now - data.get("last", 0) > 20]
            for pid in stale:
                session.presence.pop(pid, None)
            stale_typing = [pid for pid, ts in session.typing_states.items() if now - ts > 5]
            for pid in stale_typing:
                session.typing_states.pop(pid, None)
        if self.current_session_key:
            self._refresh_participants_display()
            self._refresh_typing_display()

    def _update_ui_state(self) -> None:
        has_current = self.current_session_key in self.sessions if self.current_session_key else False
        self.code_entry.configure(state="normal")
        self.name_entry.configure(state="normal")
        self.connect_button.configure(state="normal")
        self.disconnect_button.configure(state="normal" if has_current else "disabled")
        self.send_button.configure(state="normal" if has_current else "disabled")
        self.message_entry.configure(state="normal" if has_current else "disabled")

    def _on_close(self) -> None:
        for session in self.sessions.values():
            session.peer.send_typing(False)
            session.peer.stop(announce=True)
        self.sessions.clear()
        self.discovery.stop()
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tkinter LAN messenger (peer-to-peer)")
    parser.add_argument("--port", type=int, default=AUTO_ROOM_PORT, help="UDP port shared by all peers")
    parser.add_argument("--room", default=AUTO_ROOM_NAME, help="Room/channel name")
    parser.add_argument("--code", default=AUTO_ROOM_CODE, help="Private code (must match to receive)")
    parser.add_argument("--name", default="", help="Display name")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = tk.Tk()
    root.geometry("1200x800")
    app = MessengerApp(root, args.port, args.name, args.room, args.code)
    root.mainloop()


if __name__ == "__main__":
    main()
