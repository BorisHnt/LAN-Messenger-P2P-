#!/usr/bin/env python3
"""PySide6 peer-to-peer LAN messenger with multi-room support and per-room buffers."""

import argparse
import json
import queue
import socket
import threading
import uuid
import time
import sys
from collections import deque
from dataclasses import dataclass
from typing import Optional

from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QColor, QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


@dataclass

@dataclass
class RoomEntry:
    name: str
    port: int
    private: bool
    creator: str
    code: str = ""
    local: bool = False


@dataclass
class SessionState:
    key: tuple[int, str, str]
    port: int
    room: str
    code: str
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

    def announce_room(self, room: RoomEntry) -> None:
        key = self._room_key(room)
        with self.lock:
            self.rooms[key] = room
        self._announce(room)
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

    def _decode(self, data: bytes) -> Optional[dict]:
        """Decode discovery payloads (plain JSON over UDP)."""
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def _recv_loop(self) -> None:
        assert self.sock is not None
        while self.running:
            try:
                data, _addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            payload = self._decode(data)
            if not payload:
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
            existing = self.rooms.get(key)
            if existing and existing.creator == self.peer_id:
                return
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
        return f"{room.name}|{room.port}"

    def _notify(self) -> None:
        if self.on_rooms_changed:
            try:
                self.on_rooms_changed()
            except Exception:
                pass


class BroadcastPeer:
    """Sends/receives chat messages over UDP broadcast on a shared port."""

    def __init__(self, peer_id: str, session_key: tuple[int, str, str], on_message, on_presence=None, on_typing=None) -> None:
        self.session_key = session_key
        self.on_message = on_message
        self.on_presence = on_presence
        self.on_typing = on_typing
        self.sock: Optional[socket.socket] = None
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.presence_thread: Optional[threading.Thread] = None
        self.peer_id = peer_id
        self.name = ""
        self.port = 0
        self.room = "public"
        self.code = ""
        self.flood_windows: dict[str, deque] = {}
        self.flood_penalties: dict[str, float] = {}
        self.flood_limit_count = 5
        self.flood_limit_window = 3.0
        self.flood_penalty_seconds = 10.0

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

    def send_typing(self, active: bool) -> None:
        self._send({"type": "typing", "active": active})

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

            peer_id = payload.get("id")
            if peer_id == self.peer_id:
                continue

            msg_type = payload.get("type")
            name = payload.get("name", "Unknown")
            text = payload.get("text", "")
            room = payload.get("room", "public")
            code = payload.get("code", "")

            if room != self.room or code != self.code:
                continue

            if msg_type == "presence" and self.on_presence:
                self.on_presence(self.session_key, peer_id or "", name)
                continue

            if msg_type == "typing" and self.on_typing:
                self.on_typing(self.session_key, peer_id or "", name, bool(payload.get("active", False)))
                continue

            if msg_type == "chat":
                if self._is_flooding(peer_id or "", name):
                    continue
                display = f"{name}: {text}"
            elif msg_type == "system":
                if self._is_flooding(peer_id or "", name):
                    continue
                display = f"* {name} {text}"
            else:
                continue
            self.on_message(self.session_key, display)

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
        data = self._encode(payload)
        if data is None:
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
                self.on_presence(self.session_key, self.peer_id, self.name)
            self._send({"type": "presence"})
            time.sleep(5)

    def _is_flooding(self, peer_id: str, name: str) -> bool:
        now = time.time()
        penalty_end = self.flood_penalties.get(peer_id, 0.0)
        if now < penalty_end:
            return True
        window = self.flood_windows.setdefault(peer_id, deque())
        while window and now - window[0] > self.flood_limit_window:
            window.popleft()
        window.append(now)
        if len(window) > self.flood_limit_count:
            self.flood_penalties[peer_id] = now + self.flood_penalty_seconds
            if self.on_message:
                self.on_message(self.session_key, f"* Flood control: muting {name} for 10s")
            return True
        return False

    def _encode(self, payload: dict) -> Optional[bytes]:
        try:
            raw = json.dumps(payload).encode("utf-8")
        except (TypeError, ValueError):
            return None
        import base64
        return base64.b64encode(raw)

    def _decode(self, data: bytes) -> Optional[dict]:
        import base64
        try:
            decoded = base64.b64decode(data)
            return json.loads(decoded.decode("utf-8"))
        except Exception:
            return None

class MessengerWindow(QMainWindow):
    """PySide6 UI that chats with multiple rooms; each room has its own session and buffer."""

    def __init__(
        self,
        default_port: int,
        default_name: str,
        default_room: str = "public",
        default_code: str = "",
    ) -> None:
        super().__init__()
        self.setWindowTitle("LAN Messenger (PySide6)")
        self.resize(1200, 800)

        self.messages = queue.Queue()
        self.peer_id = uuid.uuid4().hex[:8]

        self.port_value = str(default_port)
        self.room_value = default_room
        self.code_value = default_code
        self.name_value = default_name

        self.discovery = DiscoveryService(self.peer_id, self._queue_rooms_update)
        self.discovery.start()

        self.rooms: list[RoomEntry] = []
        self.sessions: dict[tuple[int, str, str], SessionState] = {}
        self.current_session_key: Optional[tuple[int, str, str]] = None
        self.connected_ids: set[tuple[int, str]] = set()

        self._build_ui()
        self._refresh_rooms_from_discovery()
        self.discovery.request_rooms()

        self.poll_timer = QTimer(self)
        self.poll_timer.timeout.connect(self._poll_messages)
        self.poll_timer.start(100)

    # ---------------- UI -----------------
    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)

        splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(splitter)

        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(8, 8, 8, 8)
        splitter.addWidget(left_widget)

        label = QLabel("Rooms (port / room)")
        left_layout.addWidget(label)

        self.room_list = QListWidget()
        self.room_list.currentItemChanged.connect(self._on_room_select)
        left_layout.addWidget(self.room_list, 1)

        btn_create = QPushButton("Create Room")
        btn_create.clicked.connect(self._open_create_room)
        btn_delete = QPushButton("Delete")
        btn_delete.clicked.connect(self._delete_room)
        left_layout.addWidget(btn_create)
        left_layout.addWidget(btn_delete)

        participants_label = QLabel("Participants")
        left_layout.addWidget(participants_label)
        self.participants_list = QListWidget()
        left_layout.addWidget(self.participants_list)

        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(8, 8, 8, 8)
        splitter.addWidget(right_widget)

        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        right_layout.addLayout(top_row)

        self.port_label = QLabel(self.port_value)
        self.room_label = QLabel(self.room_value)

        top_row.addWidget(QLabel("Port"))
        top_row.addWidget(self.port_label)
        top_row.addSpacing(18)

        top_row.addWidget(QLabel("Room"))
        top_row.addWidget(self.room_label)
        top_row.addSpacing(18)

        top_row.addWidget(QLabel("Code (optional)"))
        self.code_entry = QLineEdit(self.code_value)
        self.code_entry.setFixedWidth(140)
        top_row.addWidget(self.code_entry)
        top_row.addSpacing(18)

        top_row.addWidget(QLabel("Nickname"))
        self.name_entry = QLineEdit(self.name_value)
        self.name_entry.setFixedWidth(180)
        top_row.addWidget(self.name_entry)
        top_row.addSpacing(18)

        self.connect_button = QPushButton("Connect")
        self.connect_button.clicked.connect(self._connect)
        top_row.addWidget(self.connect_button)

        self.disconnect_button = QPushButton("Disconnect")
        self.disconnect_button.clicked.connect(self._disconnect)
        top_row.addWidget(self.disconnect_button)

        top_row.addStretch()
        right_layout.addSpacing(6)

        self.text_area = QTextEdit()
        self.text_area.setReadOnly(True)
        self.text_area.setStyleSheet("background-color: #1e1e1e; color: #e6e6e6;")
        right_layout.addWidget(self.text_area, 1)

        self.typing_label = QLabel("")
        self.typing_label.setStyleSheet("color: #777;")
        right_layout.addWidget(self.typing_label)

        bottom_row = QHBoxLayout()
        self.message_entry = QLineEdit()
        self.message_entry.returnPressed.connect(self._send_message)
        self.message_entry.textEdited.connect(self._typing_event)
        bottom_row.addWidget(self.message_entry, 1)

        self.send_button = QPushButton("Send")
        self.send_button.clicked.connect(self._send_message)
        bottom_row.addWidget(self.send_button)
        right_layout.addLayout(bottom_row)

        self.status_label = QLabel("Disconnected")
        right_layout.addWidget(self.status_label)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        self._update_ui_state()

    # -------- Room list & selection ---------
    def _queue_rooms_update(self) -> None:
        self.messages.put(("rooms",))

    def _refresh_rooms_from_discovery(self) -> None:
        self.rooms = sorted(
            self.discovery.get_rooms(), key=lambda r: (r.port, r.name.lower(), r.creator)
        )
        self._refresh_room_list()

    def _refresh_room_list(self) -> None:
        self.room_list.blockSignals(True)
        self.room_list.clear()
        active_id = self.current_session_key[:2] if self.current_session_key else None
        connected_ids = set(self.connected_ids)
        for room in self.rooms:
            privacy = "private" if room.private else "public"
            suffix = " [mine]" if room.creator == self.peer_id else ""
            label = f"{room.name} @ {room.port} ({privacy}){suffix}"
            item = QListWidgetItem(label)
            rid = (room.port, room.name)
            if active_id and rid == active_id:
                item.setForeground(QColor("#0b3d91"))
            elif rid in connected_ids:
                item.setForeground(QColor("#228b22"))
            else:
                item.setForeground(QColor("black"))
            self.room_list.addItem(item)
        self.room_list.blockSignals(False)

    def _on_room_select(self, _current, _previous=None) -> None:
        idx = self.room_list.currentRow()
        if idx < 0 or idx >= len(self.rooms):
            return
        room = self.rooms[idx]
        self.port_value = str(room.port)
        self.room_value = room.name
        self.port_label.setText(self.port_value)
        self.room_label.setText(self.room_value)
        self._refresh_participants_display()
        self._refresh_room_list()
        for key in self.sessions:
            if (key[0], key[1]) == (room.port, room.name):
                self._set_active_session(key)
                return

    # ------------- Session helpers ---------------
    def _set_active_session(self, key: tuple[int, str, str]) -> None:
        self.current_session_key = key
        session = self.sessions.get(key)
        if session:
            self.port_value = str(session.port)
            self.room_value = session.room
            self.port_label.setText(self.port_value)
            self.room_label.setText(self.room_value)
            self._load_buffer(session)
            self._refresh_participants_display()
            self._refresh_typing_display(session)
            self.status_label.setText(f"UDP {session.port} | room {session.room}")
        self._update_ui_state()
        self._refresh_room_list()

    def _load_buffer(self, session: SessionState) -> None:
        self.text_area.setPlainText("\n".join(session.buffer))
        cursor = self.text_area.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.text_area.setTextCursor(cursor)

    def _append_to_session(self, key: tuple[int, str, str], message: str) -> None:
        session = self.sessions.get(key)
        if not session:
            return
        session.buffer.append(message)
        if key == self.current_session_key:
            self.text_area.append(message)
            cursor = self.text_area.textCursor()
            cursor.movePosition(QTextCursor.MoveOperation.End)
            self.text_area.setTextCursor(cursor)

    # ------------- Networking callbacks -------------
    def _handle_peer_message(self, key: tuple[int, str, str], message: str) -> None:
        self.messages.put(("msg", key, message))

    def _handle_peer_presence(self, key: tuple[int, str, str], peer_id: str, name: str) -> None:
        self.messages.put(("presence", key, peer_id, name))

    def _handle_peer_typing(self, key: tuple[int, str, str], peer_id: str, name: str, active: bool) -> None:
        self.messages.put(("typing", key, peer_id, name, active))

    # ------------- Connect / Disconnect -------------
    def _connect(self) -> None:
        name = self.name_entry.text().strip() or "Anonymous"
        room = self.room_value.strip()
        if not room:
            QMessageBox.warning(self, "No room", "Select a room or create one first.")
            return
        code = self.code_entry.text().strip()
        try:
            port = int(self.port_value.strip())
        except ValueError:
            QMessageBox.critical(self, "Invalid port", "Port must be a number.")
            return

        key = (port, room, code)
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
            QMessageBox.critical(self, "Could not start", f"Failed to bind port: {exc}")
            return

        session = SessionState(
            key=key,
            port=port,
            room=room,
            code=code,
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
        session.presence[self.peer_id] = {"name": name, "last": time.time()}
        self.sessions[key] = session
        self.connected_ids.add((port, room))
        self._set_active_session(key)
        self._broadcast_room_advertisement(session)
        self._append_to_session(key, "Connected. Peers on this port/room will see your messages.")
        self.message_entry.setFocus()

    def _disconnect(self) -> None:
        if not self.current_session_key:
            QMessageBox.warning(self, "Not connected", "Select a connected room first.")
            return
        key = self.current_session_key
        session = self.sessions.pop(key, None)
        if session:
            session.peer.send_typing(False)
            session.peer.stop(announce=True)
        self.connected_ids.discard((key[0], key[1]))
        self.current_session_key = None
        self.status_label.setText("Disconnected")
        self.participants_list.clear()
        self.typing_label.setText("")
        self.text_area.clear()
        self._update_ui_state()
        self._refresh_room_list()

    # ------------- Message sending -------------
    def _send_message(self) -> None:
        if not self.current_session_key:
            QMessageBox.warning(self, "Not connected", "Connect to a room first.")
            return
        session = self.sessions.get(self.current_session_key)
        if not session or not session.connected:
            QMessageBox.warning(self, "Not connected", "Connect to a room first.")
            return
        now = time.time()
        if now < session.send_penalty_until:
            wait = int(session.send_penalty_until - now) + 1
            self._append_to_session(session.key, f"Flood control: wait {wait}s before sending.")
            return
        message = self.message_entry.text().strip()
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
        self.message_entry.clear()
        session.last_typing_activity = 0.0

    def _prune_outgoing(self, session: SessionState, now: float) -> None:
        while session.outgoing_times and now - session.outgoing_times[0] > session.peer.flood_limit_window:
            session.outgoing_times.popleft()

    # ------------- Typing UI -------------
    def _typing_event(self, _text: str) -> None:
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
            self.typing_label.setText("")
            return
        now = time.time()
        stale = [pid for pid, ts in session.typing_states.items() if now - ts > 5]
        for pid in stale:
            session.typing_states.pop(pid, None)
        if not session.typing_states:
            self.typing_label.setText("")
            return
        peers = session.presence
        names = []
        for pid in session.typing_states:
            if pid == self.peer_id:
                names.append(self.name_entry.text().strip() or "Me")
            elif pid in peers:
                names.append(peers[pid]["name"])
        names = [n for n in names if n]
        if not names:
            self.typing_label.setText("")
            return
        if len(names) == 1:
            self.typing_label.setText(f"[{names[0]}] is typing...")
        else:
            listed = ", ".join(f"[{n}]" for n in names)
            self.typing_label.setText(f"{listed} are typing...")

    # ------------- Presence & typing callbacks -------------
    def _on_presence(self, key: tuple[int, str, str], peer_id: str, name: str) -> None:
        session = self.sessions.get(key)
        if not session:
            return
        session.presence[peer_id] = {"name": name, "last": time.time()}
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
        self.participants_list.clear()
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
            self.participants_list.addItem(data["name"])

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
            elif kind == "rooms":
                self._refresh_rooms_from_discovery()

        self._cleanup_presence_typing()
        now = time.time()
        if self.current_session_key:
            session = self.sessions.get(self.current_session_key)
            if session:
                if session.is_typing and now - session.last_typing_activity > 3:
                    session.peer.send_typing(False)
                    session.is_typing = False
                self._refresh_typing_display(session)
        for session in list(self.sessions.values()):
            if not session.connected:
                continue
            if now - session.last_room_announce > 8:
                self._broadcast_room_advertisement(session)
                session.last_room_announce = now

    # ------------- Room advertisement -------------
    def _broadcast_room_advertisement(self, session: SessionState) -> None:
        room_entry = RoomEntry(
            name=session.room,
            port=session.port,
            private=bool(session.code),
            creator=self.peer_id,
        )
        self.discovery.announce_room(room_entry)

    # ------------- Create/Delete room -------------
    def _open_create_room(self) -> None:
        dlg = QDialog(self)
        dlg.setWindowTitle("Create Room")
        form = QFormLayout(dlg)

        port_edit = QLineEdit(self.port_value)
        room_edit = QLineEdit(self.room_value)
        code_edit = QLineEdit(self.code_entry.text())

        form.addRow("Port", port_edit)
        form.addRow("Room name", room_edit)
        form.addRow("Code (optional)", code_edit)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dlg.accept)
        buttons.rejected.connect(dlg.reject)
        form.addRow(buttons)

        if dlg.exec() != QDialog.Accepted:
            return

        room_name = room_edit.text().strip() or "public"
        code = code_edit.text().strip()
        try:
            port = int(port_edit.text().strip())
        except ValueError:
            QMessageBox.critical(self, "Invalid port", "Port must be a number.")
            return

        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            test_sock.bind(("", port))
        except OSError:
            QMessageBox.critical(self, "Port unavailable", f"Port {port} cannot be bound on this host.")
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
            creator=self.peer_id,
            local=True,
        )
        self.discovery.add_local_room(room)
        self._refresh_rooms_from_discovery()
        self.port_value = str(port)
        self.room_value = room_name
        self.port_label.setText(str(port))
        self.room_label.setText(room_name)
        self.code_entry.setText(code)

    def _delete_room(self) -> None:
        idx = self.room_list.currentRow()
        if idx < 0 or idx >= len(self.rooms):
            QMessageBox.warning(self, "No selection", "Select a room to delete.")
            return
        room = self.rooms[idx]
        if room.creator != self.peer_id:
            QMessageBox.warning(self, "Cannot delete", "Only the creator can delete this room.")
            return
        self.discovery.remove_room(room)
        self._refresh_rooms_from_discovery()

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
        self.disconnect_button.setEnabled(has_current)
        self.send_button.setEnabled(has_current)
        self.message_entry.setEnabled(has_current)
        self.connect_button.setEnabled(True)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        for session in self.sessions.values():
            session.peer.send_typing(False)
            session.peer.stop(announce=True)
        self.sessions.clear()
        self.discovery.stop()
        super().closeEvent(event)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySide6 LAN messenger (peer-to-peer)")
    parser.add_argument("--port", type=int, default=5050, help="UDP port shared by all peers")
    parser.add_argument("--room", default="public", help="Room/channel name")
    parser.add_argument("--code", default="", help="Private code (must match to receive)")
    parser.add_argument("--name", default="", help="Display name")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = QApplication(sys.argv)
    window = MessengerWindow(args.port, args.name, args.room, args.code)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
