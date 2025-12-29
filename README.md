# LAN Messenger (P2P)

Single-file LAN chat built with Python sockets (UDP broadcast). There are two GUIs: a Tkinter one (`client-tk.py`) and a PySide6/Qt one (`client-pyside.py`). Every peer runs the same script; there is no central server.

## Files
- `client-tk.py`: Tkinter peer that listens on a UDP port and broadcasts messages to the LAN.
- `client-pyside.py`: PySide6 peer with the same features, using Qt widgets.

## Install PySide6 (step by step)
1. Make sure Python is installed. On Linux, open your terminal.
2. Type this magic command and press **Enter** (copy/paste it exactly):
   ```bash
   python -m pip install --user PySide6
   ```
   - If your system uses `python3` instead of `python`, run:
     ```bash
     python3 -m pip install --user PySide6
     ```
3. Wait until it finishes and shows "Successfully installed PySide6". That’s it!

## Run (PySide6 / Qt version)
On each machine connected to the same LAN/Wi-Fi, launch in the Terminal:
```bash
python client-pyside.py
```

## Run (Tkinter version)
If you prefer Tkinter and it is available on your system:
```bash
python client-tk.py
```

## Default room
- At startup every client automatically creates and announces a public room `42 Global` on port `4242` (no code). You can still create other rooms or launch with `--port/--room/--code` to use different settings.
## Run by VSCode
- Open one client inside **VSCode** and just run it.

## UI (both versions)
- Left panel: discovered rooms (port/room) and a participants list. **Create Room** opens the room-creation window, **Delete** removes a room only if you created it and nobody is inside the room. Selecting a room shows known participants (requires the correct code for private rooms).
- Room creation window: fields for Port / Room name / Code. **Create the Room** checks the port is free on your machine, saves the room locally, and broadcasts it to peers (without revealing the code).
- Selecting a room fills the fields; set your nickname up top, then **Connect** / **Disconnect**.
- Messages area + input box at bottom.

## Notes

- This is an educational project, not a fully secure messenger. Use it only on trusted networks.
- Please use the app responsibly and respectfully toward others.
- Traffic is basically encrypted; but it can be easily seen. Use only on trusted networks.
- All peers must share the same port and room. If a `Code` is set, only peers using the identical code will see messages (it is not cryptographically secure).
- Ensure your firewall allows UDP on the chosen port.
