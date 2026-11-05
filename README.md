# ilumi

Control old iLumi Bluetooth bulbs from a computer, without the discontinued phone app.

The official app is unsupported. These bulbs still speak Bluetooth Low Energy, and this project talks to them directly. It was reverse-engineered from the Android app so a Linux machine can enroll a bulb once, then turn it on, set a color, run an effect, or open a small web page you can use from a phone on the same network.

Your enrollment file stays on your machine. It is not part of this repository.

## What you need

- Linux with a Bluetooth adapter
- Python 3
- A bulb that is powered on and in range

## How to run the web app

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 enroll.py          # first time only
python3 webapp.py
```

Open http://localhost:8765/. The page can set color, brightness, white profiles, and built-in effects, including beat, track, candle, and shuffle modes.

`enroll.py` discovers nearby bulbs, flashes each one green so you can tell them apart, and asks for a name and optional groups. It writes that to `ilumi_config.json` in this directory. Keep that file private. It holds the bulb addresses and the mesh key for your set.

The server listens on all interfaces, port 8765. On a home network you can open `http://<this-computer>:8765/` from a phone. Do not expose that port to the internet. There is no login.

## How to control bulbs from the terminal

The same enrollment works for the scripts. Every control script accepts:

- `--name kitchen`
- `--group lounge`
- `--all`
- `--mac AA:BB:CC:DD:EE:FF`
- `--fade 500` (milliseconds; color defaults to 500, on/off to 1000)
- `--no-fade`
- `--stream` (connect to every target at once; best when a group should change together)
- `--mesh` and `--proxy kitchen` (experimental mesh broadcast; less reliable than `--stream`)
- `--retries 3`

```bash
python3 on.py --all
python3 off.py --group lounge
python3 color.py 0 255 0 --name kitchen
python3 color.py 255 0 0 0 25 --all          # red, white channel 0, brightness 25/255
python3 color.py random --all
python3 whites.py                             # list profiles
python3 whites.py early_morning --all
python3 effects.py                            # list effects
python3 effects.py fireworks --group lounge
```

`unenroll.py` factory-resets a bulb and drops it from your mesh. You have to enroll it again afterwards.

```bash
python3 unenroll.py --name kitchen
```

### Clock and circadian color

The bulb can shift color temperature through the day from its own clock.

```bash
python3 circadian.py sync --all
python3 circadian.py on --all
python3 circadian.py status --name kitchen
```

Two custom looks sit on top of the hardware modes:

- `python3 whites.py core_breach` — flickering molten orange
- `python3 effects.py radiation_leak` — fast green/cyan strobe

## Other ways in

These are optional. Install only what you use.

**Microphone visualizer.** Bass drives red, treble drives blue.

```bash
python3 audio_stream.py --group lounge
```

**Fast color stream** (about 20 frames per second, no per-packet acknowledgement):

```bash
python3 stream.py --fps 20 --duration 10 --all
```

**Home Assistant** via MQTT discovery:

```bash
python3 mqtt_bridge.py --broker 192.168.1.10
```

**Art-Net / DMX.** Universe packets on UDP port 6454. Channels 1–4 map to red, green, blue, and white.

```bash
python3 artnet_stream.py --universe 0 --channel 1 --group stage
```

## Mesh

`mesh_mapper.py` walks the bulbs and can save a snapshot. `pyvis_mapper.py` turns that snapshot into `mesh_map.html`.

```bash
python3 mesh_mapper.py --save mesh_data.json
python3 pyvis_mapper.py --load mesh_data.json
```

For `--mesh`, pick a `--proxy` bulb that can hear most of the others and that sits near the computer. The map shows those links. `--stream` is still the more reliable way to change several bulbs together.

Node and group IDs are also available on `IlumiSDK` (`get_node_id`, `get_group_ids`, `set_node_id`, `add_group_id`, `del_group_id`, `clear_all_group_ids`). Treat those as advanced. A bad node id will make the bulb hard to address.

## When something fails

- The bulb must be on and close enough for a direct connection. Enrollment and the web app both use the computer's Bluetooth adapter, not the phone.
- Bumble USB permission errors (`LIBUSB_ERROR_ACCESS`) are covered in [docs/BUMBLE_SETUP.md](docs/BUMBLE_SETUP.md).
- Bluetooth permissions and the GATT notes live in `ENABLEMENT.md` and `PROTOCOL.md`.
- If an effect uploads but never starts, `effects.py` both writes the pattern and sends the start command. The pattern command id is `7` and the start command id is `8` in `ilumi_sdk.py`.

## License

[MIT](LICENSE).

This talks to the bulbs with a reverse-engineered protocol and skips the manufacturer's app. The authors are not responsible for what the hardware does. Use it on bulbs you own.
