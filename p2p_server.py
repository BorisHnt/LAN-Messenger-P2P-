#!/usr/bin/env python3
"""Networking primitives for the LAN P2P messenger (discovery + chat transport)."""

from __future__ import annotations

import base64
import json
import socket
import threading
import time
from collections import deque
from dataclasses import dataclass
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
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        except OSError:
            return
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (OSError, AttributeError):
            pass
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            sock.bind(("", self.DISCOVERY_PORT))
        except OSError:
            sock.close()
            return
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
            # Ignore external remove attempts for rooms we own.
            if key in self.local_rooms and self.local_rooms[key].creator == self.peer_id:
                return
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

    def __init__(
        self,
        peer_id: str,
        session_key: tuple[int, str, str],
        on_message,
        on_presence=None,
        on_typing=None,
    ) -> None:
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
            payload = self._decode(data)
            if not payload:
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
                try:
                    self.on_presence(self.session_key, peer_id or "", name)
                except Exception:
                    pass
                continue

            if msg_type == "typing" and self.on_typing:
                try:
                    self.on_typing(self.session_key, peer_id or "", name, bool(payload.get("active", False)))
                except Exception:
                    pass
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
            if self.on_message:
                try:
                    self.on_message(self.session_key, display)
                except Exception:
                    pass

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
        return base64.b64encode(raw)

    def _decode(self, data: bytes) -> Optional[dict]:
        try:
            decoded = base64.b64decode(data)
            return json.loads(decoded.decode("utf-8"))
        except Exception:
            return None
