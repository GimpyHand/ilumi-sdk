import os as _os
import asyncio
import struct
import time
import logging
import sys
from typing import List, Optional, Dict, Any, Callable
from bleak import BleakClient, BleakScanner
import config

if _os.environ.get("ILUMI_USE_BUMBLE"):
    from bumble_sdk import (  # noqa: F401
        IlumiSDK, IlumiConnectionError, IlumiProtocolError,
        IlumiApiCmdType, IlumiConfigCmdType,
        ILUMI_SERVICE_UUID, ILUMI_API_CHAR_UUID,
        execute_on_targets,
    )
    import sys as _sys
    _sys.modules[__name__] = _sys.modules["bumble_sdk"]
    # We don't raise SystemExit here so that the module remains available
    # but redirects to the bumble version.

# Configure logging to stderr to avoid corrupting stdout (used for JSON output)
logger = logging.getLogger("ilumi_sdk")
_handler = logging.StreamHandler(sys.stderr)
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel(logging.INFO)

ILUMI_SERVICE_UUID = "f000f0c0-0451-4000-b000-000000000000"
ILUMI_API_CHAR_UUID = "f000f0c1-0451-4000-b000-000000000000"

# Shared global state for serialization
_device_lock = asyncio.Lock()

class CommandQueue:
    """Manages serial execution of BLE commands to avoid adapter congestion."""
    def __init__(self, max_depth: int = 5):
        self.queue = asyncio.Queue()
        self.max_depth = max_depth
        self.worker_task = None

    async def _worker(self):
        while True:
            item = await self.queue.get()
            coro, future = item
            try:
                if future.done():
                    # Coroutine was cancelled/dropped while in queue
                    try: coro.close()
                    except: pass
                    continue

                result = await coro
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                if not future.done():
                    future.set_exception(e)
            finally:
                self.queue.task_done()

    def start(self):
        if self.worker_task is None or self.worker_task.done():
            self.worker_task = asyncio.create_task(self._worker())

    async def execute(self, coro, high_priority: bool = False):
        """Adds a command to the queue. Returns the result or raises exception."""
        self.start()
            
        # If queue is full and this is a stream command, drop oldest to keep up
        if not high_priority and self.queue.qsize() >= self.max_depth:
            try:
                dropped_item = self.queue.get_nowait()
                _, dropped_future = dropped_item
                if not dropped_future.done():
                    dropped_future.set_exception(TimeoutError("Dropped from queue"))
                self.queue.task_done()
            except asyncio.QueueEmpty:
                pass
                
        future = asyncio.get_running_loop().create_future()
        await self.queue.put((coro, future))
        return await future

_command_queue = CommandQueue()

def reset_global_state():
    """Resets global locks and queues. Primarily for testing with fresh loops."""
    global _command_queue, _device_lock
    _command_queue = CommandQueue()
    _device_lock = asyncio.Lock()

class IlumiConnectionError(Exception):
    """Raised when failing to connect to an Ilumi bulb."""
    pass

class IlumiProtocolError(Exception):
    """Raised when encountering unexpected data from the Ilumi protocol."""
    pass

class IlumiApiCmdType:
    """Mapping of Ilumi GATT API command IDs."""
    ILUMI_API_CMD_SET_COLOR = 0
    ILUMI_API_CMD_SET_DEAULT_COLOR = 1
    ILUMI_API_CMD_TURN_ON = 4
    ILUMI_API_CMD_TURN_OFF = 5
    ILUMI_API_CMD_SET_COLOR_PATTERN = 7
    ILUMI_API_CMD_START_COLOR_PATTERN = 8
    ILUMI_API_CMD_SET_DAILY_ALARM = 9
    ILUMI_API_CMD_SET_CALENDAR_EVENT = 10
    ILUMI_API_CMD_SET_DATE_TIME = 12
    ILUMI_API_CMD_GET_DATE_TIME = 13
    ILUMI_API_CMD_DELETE_ALARM = 14
    ILUMI_API_CMD_DELETE_ALL_ALARMS = 15
    ILUMI_API_GET_BULB_COLOR = 16
    ILUMI_API_CMD_DELETE_COLOR_PATTERN = 18
    ILUMI_API_CMD_DELETE_ALL_COLOR_PATTERNS = 19
    ILUMI_API_CMD_CLEAR_ALL_USER_DATA = 20
    ILUMI_API_CMD_SET_NODE_ID = 21
    ILUMI_API_CMD_GET_NODE_ID = 22
    ILUMI_API_CMD_ADD_GROUP_ID = 23
    ILUMI_API_CMD_DEL_GROUP_ID = 24
    ILUMI_API_CMD_GET_GROUP_IDS = 25
    ILUMI_API_CMD_CLEAR_ALL_GROUP_IDS = 26
    ILUMI_API_CMD_PROXY_MSG = 28
    ILUMI_API_CMD_QUERY_ROUTING = 31
    ILUMI_API_CMD_SET_CANDL_MODE = 35
    ILUMI_API_CMD_SET_COLOR_SMOOTH = 37
    ILUMI_API_CMD_RANDOM_COLOR_SEQUENCE = 38
    ILUMI_API_CMD_HEARTBEAT = 39
    ILUMI_API_CMD_GET_DEVICE_INFO = 40
    ILUMI_API_CMD_ENABLE_CIRCADIAN = 42
    ILUMI_API_CMD_SET_RANDOM_COLOR = 48
    ILUMI_API_CMD_ADD_ACTION = 50
    ILUMI_API_CMD_DEL_ACTION = 51
    ILUMI_API_CMD_DATA_CHUNK = 52
    ILUMI_API_CMD_SET_COLOR_NEED_RESP = 54
    ILUMI_API_CMD_SET_DEFAULT_ACTION_IDX = 56
    ILUMI_API_CMD_COMMISSION_WITH_ID = 58
    ILUMI_API_CMD_SET_BRIGHTNESS = 61
    ILUMI_API_CMD_CONFIG = 65
    ILUMI_API_CMD_TREE_MESH_PROXY = 68
    ILUMI_API_CMD_GET_HARDWARE_TYPE = 70
    ILUMI_API_CMD_GET_ALARM_DATA = 75
    ILUMI_API_CMD_PING = 84
    ILUMI_API_CMD_PING_ECHO = 85

class IlumiConfigCmdType:
    """Mapping of commands within the ILUMI_API_CMD_CONFIG type."""
    ILUMI_CONFIG_ENTER_BOOTLOADER = 2

class IlumiSDK:
    """
    Main SDK class for interacting with Ilumi Smart Bulbs via BLE.
    Supports direct control, mesh proxying, and device management.
    """
    ILUMI_AQUA = (0, 255, 255, 0, 255)

    # Hardware-backed Circadian profile mapping (extracted from Java SDK)
    CIRCADIAN_PROFILE = {
        "05:30": "8281FF", # Light Lavender/Blue
        "11:00": "D7EFFF", # Bright Sky Blue
        "14:00": "D8FFAA", # Lime/Yellow Tint
        "17:00": "BAFF4D", # Lime
        "20:00": "FEFF0E", # Yellow
        "22:00": "FFA700"  # Orange
    }

    def __init__(self, mac_address: Optional[str] = None):
        """
        Initialize the SDK.
        :param mac_address: MAC address of the target bulb. If None, uses value from config.
        """
        # Normalize MAC address to remove Bumble suffixes (/P, /R) if present
        import config
        self.mac_address = config.normalize_mac(mac_address or config.get_config("mac_address"))
        self.network_key = config.get_config("network_key", 0)
        self.seq_num = config.get_config("seq_num", 0)
        self.dfu_key = config.get_config("dfu_key", 0x12345678)
        self.client: Optional[BleakClient] = None
        self._ble_device = None  # can be set to a BLEDevice to skip per-connect scan
        self._last_device_info: Optional[Dict[str, Any]] = None
        self._last_color: Optional[Dict[str, int]] = None
        self._device_info_event = asyncio.Event()
        self._color_event = asyncio.Event()
        self._mesh_info: List[Dict[str, Any]] = []
        self._mesh_event = asyncio.Event()
        self._get_alarm_data_event = asyncio.Event()
        self._last_alarm_data: Optional[bytes] = None
        self._circadian_state: Optional[bool] = None
        self._circadian_event = asyncio.Event()
        self._ping_event = asyncio.Event()
        self._last_ping_payload: Optional[bytes] = None
        self._last_node_id: Optional[int] = None
        self._node_id_event = asyncio.Event()
        self._last_group_ids: List[int] = []
        self._group_ids_event = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        """Returns True if the SDK is connected to a bulb."""
        return self.client is not None and self.client.is_connected

    async def __aenter__(self):
        """Context manager to maintain a persistent connection."""
        if not self.mac_address:
            raise ValueError("No MAC address specified or enrolled.")
        
        if self.is_connected:
            return self

        async with _device_lock:
            # Re-check inside lock to be absolutely certain
            if self.is_connected:
                return self
                
            target = self._ble_device if self._ble_device is not None else self.mac_address
            self.client = BleakClient(target, timeout=10.0)
            logger.info(f"Connecting to {self.mac_address}...")
            try:
                await self.client.connect()
            except Exception as e:
                raise IlumiConnectionError(f"Failed to connect to {self.mac_address}: {e}")
        
        def notification_handler(sender, data: bytearray):
            logger.debug(f"GATT Notification: {data.hex()}")
            if len(data) >= 2:
                cmd_type = data[0]
                
                if cmd_type == IlumiApiCmdType.ILUMI_API_CMD_PROXY_MSG:
                    if len(data) >= 10:
                        inner_len = struct.unpack("<H", data[8:10])[0]
                        if inner_len > 0:
                            inner_payload = data[10:10+inner_len]
                            inner_type = inner_payload[0]
                            if inner_type == IlumiApiCmdType.ILUMI_API_GET_BULB_COLOR:
                                if len(inner_payload) >= 6:
                                    self._last_color = {
                                        "r": inner_payload[1],
                                        "g": inner_payload[2],
                                        "b": inner_payload[3],
                                        "w": inner_payload[4],
                                        "brightness": inner_payload[5]
                                    }
                                    self._color_event.set()
                            elif inner_type == IlumiApiCmdType.ILUMI_API_CMD_ENABLE_CIRCADIAN:
                                if len(inner_payload) >= 2:
                                    self._circadian_state = bool(inner_payload[1])
                                    self._circadian_event.set()
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_CMD_GET_DEVICE_INFO:
                    if len(data) >= 14:
                        try:
                            unpacked = struct.unpack("<H H B B H H", data[4:14])
                            self._last_device_info = {
                                "firmware_version": unpacked[0],
                                "bootloader_version": unpacked[1],
                                "commission_status": unpacked[2],
                                "model_number": unpacked[3],
                                "reset_reason": unpacked[4],
                                "ble_stack_version": unpacked[5]
                            }
                            self._device_info_event.set()
                        except Exception as e:
                            logger.error(f"Failed to parse device info: {e}")
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_GET_BULB_COLOR:
                    if len(data) >= 9:
                        self._last_color = {
                            "r": data[4],
                            "g": data[5],
                            "b": data[6],
                            "w": data[7],
                            "brightness": data[8]
                        }
                        self._color_event.set()
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_CMD_ENABLE_CIRCADIAN:
                    # Payload is 1 byte: 1 for enabled, 0 for disabled
                    if len(data) >= 5:
                        self._circadian_state = bool(data[4])
                        self._circadian_event.set()
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_CMD_QUERY_ROUTING:
                    if len(data) >= 4:
                        payload_size = struct.unpack("<H", data[2:4])[0]
                        payload = data[4:4+payload_size]
                        
                        # Each entry is 8 bytes: MAC(6), Hops(1), RSSI(1)
                        entry_size = 8
                        for i in range(len(payload) // entry_size):
                            entry = payload[i*entry_size : (i+1)*entry_size]
                            if len(entry) < 8: continue
                            mac_bytes = entry[0:6]
                            hops = entry[6]
                            rssi = struct.unpack('b', entry[7:8])[0]
                            # MAC is little-endian in payload
                            mac_str = ":".join(f"{b:02X}" for b in mac_bytes[::-1])
                            self._mesh_info.append({
                                "address": mac_str,
                                "hops": hops,
                                "rssi": rssi
                            })
                        
                        # Total size check to see if we should signal completion
                        # For now, we signal after each block since we don't know the total
                        # The high-level method will wait and collect for a bit.
                        self._mesh_event.set()
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_CMD_GET_ALARM_DATA:
                    if len(data) >= 4:
                        self._last_alarm_data = bytes(data[4:])
                        self._get_alarm_data_event.set()
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_CMD_PING_ECHO:
                    if len(data) >= 4:
                        self._last_ping_payload = bytes(data[4:])
                        self._ping_event.set()
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_CMD_GET_NODE_ID:
                    if len(data) >= 6:
                        self._last_node_id = struct.unpack("<H", data[4:6])[0]
                        self._node_id_event.set()
                
                elif cmd_type == IlumiApiCmdType.ILUMI_API_CMD_GET_GROUP_IDS:
                    if len(data) >= 4:
                        payload_size = struct.unpack("<H", data[2:4])[0]
                        payload = data[4:4+payload_size]
                        # Each Group ID is 2 bytes (16-bit)
                        self._last_group_ids = []
                        for i in range(0, len(payload), 2):
                            if i + 2 <= len(payload):
                                group_id = struct.unpack("<H", payload[i:i+2])[0]
                                self._last_group_ids.append(group_id)
                        self._group_ids_event.set()

        await self.client.start_notify(ILUMI_API_CHAR_UUID, notification_handler)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up the persistent connection."""
        if self.client:
            logger.info(f"Closing connection to {self.mac_address}...")
            try:
                await self.client.stop_notify(ILUMI_API_CHAR_UUID)
                await self.client.disconnect()
            except Exception as e:
                logger.error(f"Error during disconnect: {e}")
            self.client = None

    @staticmethod
    async def discover(timeout: float = 5.0) -> List[Dict[str, Any]]:
        """
        Scans for available Ilumi bulbs.
        :param timeout: How long to scan for.
        :return: List of discovered bulbs with name and address.
        """
        logger.info(f"Scanning for Ilumi bulbs ({timeout}s)...")
        # Bleak 3+: discover() returns BLEDevice without .rssi; use return_adv for AdvertisementData.
        discovered = await BleakScanner.discover(timeout=timeout, return_adv=True)
        ilumi_bulbs = []
        if isinstance(discovered, dict):
            items = discovered.values()
        else:
            items = [(d, None) for d in discovered]
        for d, adv in items:
            name = d.name or (getattr(adv, "local_name", None) if adv else None)
            if not name:
                continue
            if "ilumi" in name.lower() or name.startswith("L0") or name.startswith("Nrdic"):
                rssi = getattr(adv, "rssi", None) if adv else getattr(d, "rssi", None)
                ilumi_bulbs.append({"name": name, "address": d.address, "rssi": rssi})
        return ilumi_bulbs

    def _pack_header(self, message_type: int) -> bytes:
        """
        Packs the standard 6-byte Ilumi header.
        :param message_type: The IlumiApiCmdType ID.
        """
        network_id_bytes = struct.pack("<I", self.network_key)
        
        # Correct order as per insertNetworkKey_SeqnumForNodeMac in IlumiSDK.java:
        # 0-3: network_key (4, LE)
        # 4: seq_num (1)
        # 5: message_type (1)
        
        header = network_id_bytes + struct.pack("B B", self.seq_num, message_type)
        
        self.seq_num = (self.seq_num + 2) % 256
        # Persist occasionally — every packet used to fsync JSON and made streaming color slow.
        if self.seq_num % 32 == 0:
            config.update_config("seq_num", self.seq_num)
        
        return header

    async def _write(self, payload: bytes, with_response: bool = True):
        """INTERNAL: Serialized GATT write."""
        if not self.client or not self.client.is_connected:
            raise IlumiConnectionError("Not connected")

        async def do_write():
            if self.client and self.client.is_connected:
                await self.client.write_gatt_char(ILUMI_API_CHAR_UUID, payload, response=with_response)

        await _command_queue.execute(do_write(), high_priority=with_response)

    async def _send_command(self, payload: bytes):
        """Sends a command with GATT write response expectation."""
        if self.client and self.client.is_connected:
            await self._write(payload, with_response=True)
            await asyncio.sleep(0.1)
            return

        async with self as managed_sdk:
            await managed_sdk._write(payload, with_response=True)
            await asyncio.sleep(0.5)

    async def _send_command_fast(self, payload: bytes):
        """Sends a command without waiting for GATT response (write command)."""
        if self.client and self.client.is_connected:
            await self._write(payload, with_response=False)
            return

        async with self as managed_sdk:
            await managed_sdk._write(payload, with_response=False)

    async def _send_chunked_command(self, data: bytes):
        """Splits payloads > 20 bytes into 10-byte fragments for reliable BLE transfer."""
        data_length = len(data)
        if data_length <= 20:
            await self._send_command(data)
            return

        async def _do_send(target_client):
            for offset in range(0, data_length, 10):
                chunk_size = min(10, data_length - offset)
                chunk_payload = bytearray(10)
                chunk_payload[0:chunk_size] = data[offset:offset+chunk_size]
    
                chunk_struct = struct.pack("<H H 10s", data_length, offset, bytes(chunk_payload))
                cmd_header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_DATA_CHUNK)
                logger.debug(f"Sending chunk {offset}/{data_length}")
                await target_client._write(cmd_header + chunk_struct, with_response=True)
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.5)

        if self.client and self.client.is_connected:
            await _do_send(self)
        else:
            async with self as managed_sdk:
                await _do_send(managed_sdk)

    async def _send_raw_command(self, cmd_type: int, payload: bytes):
        """
        INTERNAL UNSAFE METHOD: Sends a raw command type and payload.
        > [!CAUTION]
        > **BRICKING RISK**: Sending invalid commands via CONFIG or COMMISSION types 
        > can permanently disable or lockout the bulb. Use with extreme caution.
        """
        header = self._pack_header(cmd_type)
        logger.warning(f"Sending RAW command type {cmd_type} with payload: {payload.hex()}")
        await self._send_chunked_command(header + payload)

    async def send_proxy_message(self, target_macs: List[str], inner_payload: bytes):
        """Routes an inner API payload to a list of target MAC addresses via the mesh."""
        import config
        for target_mac in target_macs:
            # Normalize to remove Bumble suffixes (/P, /R)
            normalized_mac = config.normalize_mac(target_mac)
            mac_parts = [int(x, 16) for x in normalized_mac.split(':')]
            mac_parts.reverse() 
            mac_bytes = bytes(mac_parts)
            
            service_type_ttl = 47 if len(inner_payload) <= 17 else 15
            addr_amount = 1
            proxy_data_len = 6 + len(inner_payload)
            
            proxy_cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_PROXY_MSG)
            proxy_header = struct.pack("<B B H", service_type_ttl, addr_amount, proxy_data_len)
            
            final_payload = proxy_cmd + proxy_header + mac_bytes + inner_payload
            await self._send_chunked_command(final_payload)
            await asyncio.sleep(0.1)

    async def commission(self, new_network_key: int, group_id: int, node_id: int) -> bool:
        """Assigns a network key and bulb ID. Returns success."""
        self.network_key = new_network_key
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_COMMISSION_WITH_ID)
        payload = struct.pack("<I H H", new_network_key, node_id, group_id)
        
        try:
            await self._send_command(cmd + payload)
            if new_network_key != 0:
                config.update_config("network_key", new_network_key)
                config.update_config("group_id", group_id)
                config.update_config("node_id", node_id)
            return True
        except Exception as e:
            logger.error(f"Failed to commission: {e}")
            return False

    async def turn_on(self, delay: int = 0, transit: int = 0, targets: Optional[List[str]] = None):
        """Turns the bulb(s) on with optional fade (transit)."""
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_TURN_ON)
        payload = struct.pack("<H H", delay, transit)
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command(cmd + payload)

    async def turn_off(self, delay: int = 0, transit: int = 0, targets: Optional[List[str]] = None):
        """Turns the bulb(s) off with optional fade (transit)."""
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_TURN_OFF)
        payload = struct.pack("<H H", delay, transit)
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command(cmd + payload)

    async def set_color(self, r: int, g: int, b: int, w: int = 0, brightness: int = 255, targets: Optional[List[str]] = None):
        """Sets an instant color. Clamp values to 0-255."""
        clamp = lambda x: max(0, min(255, int(x)))
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_COLOR_NEED_RESP)
        payload = struct.pack("<B B B B B B B", clamp(r), clamp(g), clamp(b), clamp(w), clamp(brightness), 0, 0)
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command(cmd + payload)

    async def set_color_smooth(self, r: int, g: int, b: int, w: int = 0, brightness: int = 255, duration_ms: int = 500, delay_sec: int = 0, targets: Optional[List[str]] = None):
        """Fades to a color over a specific duration."""
        clamp = lambda x: max(0, min(255, int(x)))
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_COLOR_SMOOTH)
        
        if duration_ms < 65535:
            time_val = int(duration_ms)
            time_unit = 0 
        else:
            time_val = int(duration_ms / 1000)
            time_unit = 1 
            
        payload = struct.pack("<H B B B B B B B B", 
                              time_val, time_unit,
                              clamp(r), clamp(g), clamp(b), clamp(w), clamp(brightness), 0,
                              clamp(delay_sec))
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command(cmd + payload)

    async def set_color_fast(self, r: int, g: int, b: int, w: int = 0, brightness: int = 255, targets: Optional[List[str]] = None):
        """Sets color without waiting for BLE acknowledgement."""
        clamp = lambda x: max(0, min(255, int(x)))
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_COLOR)
        payload = struct.pack("<B B B B B B B", clamp(r), clamp(g), clamp(b), clamp(w), clamp(brightness), 0, 0)
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command_fast(cmd + payload)

    async def set_candle_mode(self, r: int, g: int, b: int, w: int = 0, brightness: int = 255, targets: Optional[List[str]] = None):
        """Activates flickering candle mode."""
        clamp = lambda x: max(0, min(255, int(x)))
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_CANDL_MODE)
        payload = struct.pack("<B B B B B B B", clamp(r), clamp(g), clamp(b), clamp(w), clamp(brightness), 0, 0)
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command(cmd + payload)

    async def set_color_pattern(self, scene_idx: int, frames: List[Dict[str, int]], repeatable: int = 1, start_now: int = 1, targets: Optional[List[str]] = None):
        """Uploads and triggers an animation pattern."""
        clamp = lambda x: max(0, min(255, int(x)))
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_COLOR_PATTERN)
        frame_bytes = bytearray()
        for f in frames:
            color = struct.pack("<B B B B B B", clamp(f.get('r', 0)), clamp(f.get('g', 0)), clamp(f.get('b', 0)), clamp(f.get('w', 0)), clamp(f.get('brightness', 255)), 0)
            timings = struct.pack("<I I B B B B", f.get('sustain_ms', 500), f.get('transit_ms', 100), 0, 0, 0, 0)
            frame_bytes.extend(color + timings)

        total_struct_size = 13 + len(frame_bytes) 
        array_size = len(frames)
        scene_header = struct.pack("<H B B B B B", total_struct_size, scene_idx, array_size, repeatable, scene_idx, start_now)
        full_payload = cmd + scene_header + frame_bytes
        
        if targets:
            await self.send_proxy_message(targets, full_payload)
        else:
            await self._send_chunked_command(full_payload)

    async def start_color_pattern(self, scene_idx: int):
        """Starts a previously uploaded animation scene."""
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_START_COLOR_PATTERN)
        payload = struct.pack("<B", scene_idx)
        await self._send_command(cmd + payload)

    async def get_bulb_color(self, targets: Optional[List[str]] = None) -> Optional[Dict[str, int]]:
        """Queries current bulb color status."""
        self._last_color = None
        self._color_event = asyncio.Event()
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_GET_BULB_COLOR)
        
        if targets:
            await self.send_proxy_message(targets, header)
        else:
            await self._send_command(header)
        
        try:
            await asyncio.wait_for(self._color_event.wait(), timeout=5.0)
            return self._last_color
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for color status.")
            return None

    async def get_device_info(self) -> Optional[Dict[str, Any]]:
        """Retrieves hardware and firmware information."""
        self._device_info_event.clear()
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_GET_DEVICE_INFO)
        await self._send_command(cmd)
        
        try:
            await asyncio.wait_for(self._device_info_event.wait(), timeout=10.0)
            return self._last_device_info
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for device info.")
            return None

    async def enter_dfu_mode(self) -> None:
        """Triggers bootloader mode for firmware updates."""
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_CONFIG)
        payload = struct.pack("<B 4B", IlumiConfigCmdType.ILUMI_CONFIG_ENTER_BOOTLOADER, 
                              self.dfu_key & 0xFF, (self.dfu_key >> 8) & 0xFF, 
                              (self.dfu_key >> 16) & 0xFF, (self.dfu_key >> 24) & 0xFF)
        
        logger.info(f"Sending DFU mode entry command with key: {hex(self.dfu_key)}")
        await self._send_command(cmd + payload)

    async def set_circadian(self, enabled: bool, targets: Optional[List[str]] = None):
        """
        Enables or disables the hardware-native circadian rhythm mode.
        :param enabled: True to enable, False to disable.
        :param targets: Optional list of target MAC addresses for mesh proxying.
        """
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_ENABLE_CIRCADIAN)
        payload = struct.pack("<B", 1 if enabled else 0)
        
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command(cmd + payload)

    async def sync_time(self, timestamp: Optional[float] = None, targets: Optional[List[str]] = None):
        """
        Synchronizes the bulb's internal clock with the provided timestamp or current system time.
        :param timestamp: Optional Unix timestamp (float). If None, uses local system time.
        :param targets: Optional list of target MAC addresses for mesh proxying.
        """
        import time
        now = int(timestamp if timestamp is not None else time.time())
        cmd = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_DATE_TIME)
        payload = struct.pack("<I", now)
        
        logger.info(f"Synchronizing bulb clock to {now}...")
        if targets:
            await self.send_proxy_message(targets, cmd + payload)
        else:
            await self._send_command(cmd + payload)

    async def upload_circadian_profile(self, profile: Optional[Dict[str, str]] = None, scene_idx: int = 1, timestamp: Optional[float] = None, targets: Optional[List[str]] = None):
        """
        Calculate and upload a 24-hour circadian profile, reordered to start from 'now'.
        If profile is None, uses the default CIRCADIAN_PROFILE.
        Profile should be a dict of "HH:MM": "HEXCOLOR".
        :param timestamp: Optional Unix timestamp to use for 'now'. If None, uses local system time.
        """
        import time
        if profile is None:
            profile = self.CIRCADIAN_PROFILE
            
        sorted_times = sorted(profile.keys())
        
        def time_to_seconds(ts):
            h, m = map(int, ts.split(':'))
            return h * 3600 + m * 60
            
        # Determine "now" in seconds since midnight
        lt = time.localtime(timestamp) if timestamp is not None else time.localtime()
        now_sec = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec
        
        times_sec = [time_to_seconds(ts) for ts in sorted_times]
        
        # Find the point immediately before/at now
        current_idx = -1
        for i in range(len(times_sec)):
            if times_sec[i] <= now_sec:
                current_idx = i
            else:
                break
        
        if current_idx == -1:
            current_idx = len(times_sec) - 1 # Before the first point, so last point of prev day applies
            
        next_idx = (current_idx + 1) % len(times_sec)
        
        # Create ordered sequence of indices starting from next_idx
        ordered_indices = []
        for j in range(len(times_sec)):
            ordered_indices.append((next_idx + j) % len(times_sec))
            
        frames = []
        for k in range(len(ordered_indices)):
            idx = ordered_indices[k]
            prev_idx = (idx - 1) % len(times_sec)
            
            t_curr = times_sec[idx]
            t_prev = times_sec[prev_idx]
            
            if k == 0:
                # First frame: transition from NOW to t_curr
                if t_curr <= now_sec:
                    duration = (24 * 3600 - now_sec) + t_curr
                else:
                    duration = t_curr - now_sec
            else:
                # Subsequent frames: transition from prev to curr
                if t_curr <= t_prev:
                    duration = (24 * 3600 - t_prev) + t_curr
                else:
                    duration = t_curr - t_prev
            
            hex_color = profile[sorted_times[idx]]
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)
            
            frames.append({
                'r': r, 'g': g, 'b': b, 'w': 0, 'brightness': 255,
                'sustain_ms': 0,
                'transit_ms': int(duration * 1000)
            })
            
        # 1. Set base color to the 'current' point for a smooth transition start
        cur_hex = profile[sorted_times[current_idx]]
        cr, cg, cb = int(cur_hex[0:2], 16), int(cur_hex[2:4], 16), int(cur_hex[4:6], 16)
        logger.info(f"Setting base circadian color to {cur_hex} (point {sorted_times[current_idx]})")
        await self.set_color(cr, cg, cb, 0, 255, targets=targets)
        await asyncio.sleep(0.5)
        
        logger.info(f"Uploading reordered circadian profile starting with transition to {sorted_times[next_idx]}.")
        return await self.set_color_pattern(scene_idx, frames, repeatable=255, start_now=1, targets=targets)

    async def get_circadian(self, target: Optional[str] = None) -> Optional[bool]:
        """
        Queries the current circadian rhythm state from the bulb.
        :param target: Optional target MAC address for mesh proxying.
        :return: True if enabled, False if disabled, None if timeout.
        """
        self._circadian_state = None
        self._circadian_event.clear()
        
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_ENABLE_CIRCADIAN)
        # Query mode: send command header with no payload
        
        if target:
            await self.send_proxy_message([target], header)
        else:
            await self._send_command(header)
            
        try:
            await asyncio.wait_for(self._circadian_event.wait(), timeout=5.0)
            return self._circadian_state
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for circadian status.")
            return None

    async def get_mesh_info(self) -> List[Dict[str, Any]]:
        """
        Retrieves neighbor mesh information from the bulb.
        Returns a list of neighbors with MAC addresses and RSSI values.
        """
        self._mesh_info = []
        self._mesh_event.clear()
        
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_QUERY_ROUTING)
        logger.info(f"Querying mesh routing info from {self.mac_address}...")
        await self._send_command(header)
        
        try:
            # We wait for multiple notifications as mesh tables can be large.
            # We'll wait until no new data arrives for 1 second, or total timeout.
            while True:
                try:
                    await asyncio.wait_for(self._mesh_event.wait(), timeout=1.5)
                    self._mesh_event.clear()
                except asyncio.TimeoutError:
                    break
            
            return self._mesh_info
        except Exception as e:
            logger.error(f"Error retrieving mesh info: {e}")
            return self._mesh_info

    @staticmethod
    def _bin_to_bcd(val: int) -> int:
        """Converts an integer to Binary Coded Decimal (BCD) byte format."""
        return ((val // 10) << 4) | (val % 10)

    async def add_action(self, action_idx: int, command_payload: bytes, next_action_idx: int = 0xFF, delay_ms: int = 0):
        """
        Stores an autonomous hardware action (macro) on the bulb.
        :param action_idx: Unique index for this action (0-255).
        :param command_payload: Raw Ilumi command payload (EXCLUDING the 6-byte header).
        :param next_action_idx: Next action to trigger after this one (0xFF for none).
        :param delay_ms: Delay before executing the next action.
        """
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_ADD_ACTION)
        
        # Determine unit and value for delay
        if delay_ms < 65535:
            time_val = delay_ms
            time_unit = 0 # ms
        else:
            time_val = delay_ms // 1000
            time_unit = 1 # seconds

        # action_idx(1), next_action_idx(1), interval(2), unit(1), timer_start_after_current_done(1), data_length(2)
        # timer_start_after_current_done: 0 means start timer immediately. 1 means wait for current command to finish.
        action_hdr = struct.pack("<B B H B B H", action_idx, next_action_idx, time_val, time_unit, 0, len(command_payload))
        
        logger.info(f"Uploading hardware action {action_idx} (payload len: {len(command_payload)})...")
        await self._send_chunked_command(header + action_hdr + command_payload)

    async def delete_color_pattern(self, scene_idx: int):
        """Deletes a specific color pattern (scene)."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_DELETE_COLOR_PATTERN)
        payload = struct.pack("<B", scene_idx)
        await self._send_command(header + payload)

    async def get_node_id(self) -> Optional[int]:
        """Queries the bulb's current Node ID."""
        self._last_node_id = None
        self._node_id_event.clear()
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_GET_NODE_ID)
        await self._send_command(header)
        try:
            await asyncio.wait_for(self._node_id_event.wait(), timeout=5.0)
            return self._last_node_id
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for Node ID.")
            return None

    async def set_node_id(self, node_id: int):
        """
        Sets a new Node ID for the bulb.
        WARNING: This can affect mesh routing. Use with caution.
        """
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_NODE_ID)
        payload = struct.pack("<H", node_id)
        logger.warning(f"Setting Node ID to {node_id}...")
        await self._send_command(header + payload)

    async def get_group_ids(self) -> List[int]:
        """Queries the list of group IDs this bulb belongs to."""
        self._last_group_ids = []
        self._group_ids_event.clear()
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_GET_GROUP_IDS)
        await self._send_command(header)
        try:
            await asyncio.wait_for(self._group_ids_event.wait(), timeout=5.0)
            return self._last_group_ids
        except asyncio.TimeoutError:
            logger.error("Timed out waiting for Group IDs.")
            return []

    async def add_group_id(self, group_id: int):
        """Adds this bulb to a specific group."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_ADD_GROUP_ID)
        payload = struct.pack("<H", group_id)
        logger.info(f"Adding bulb to group {group_id}...")
        await self._send_command(header + payload)

    async def del_group_id(self, group_id: int):
        """Removes this bulb from a specific group."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_DEL_GROUP_ID)
        payload = struct.pack("<H", group_id)
        logger.info(f"Removing bulb from group {group_id}...")
        await self._send_command(header + payload)

    async def clear_all_group_ids(self):
        """Removes this bulb from all groups."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_CLEAR_ALL_GROUP_IDS)
        logger.info("Clearing all group memberships...")
        await self._send_command(header)


    async def delete_all_color_patterns(self):
        """Deletes all custom color patterns."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_DELETE_ALL_COLOR_PATTERNS)
        await self._send_command(header)

    async def clear_all_user_data(self):
        """Unenrolls the bulb and resets all user data (Manufacturer Reset)."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_CLEAR_ALL_USER_DATA)
        await self._send_command(header)

    async def set_random_color(self):
        """Sets the bulb to a random color."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_RANDOM_COLOR)
        await self._send_command(header)

    async def random_color_sequence(self):
        """Starts a sequence of random colors."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_RANDOM_COLOR_SEQUENCE)
        await self._send_command(header)

    async def set_daily_alarm(self, alarm_idx: int, hour: int, minute: int, days_mask: int, action_idx: int):
        """
        Schedules a recurring daily alarm to trigger a stored action.
        This method automatically converts the local time to GMT for the bulb's internal clock.
        :param alarm_idx: Unique index for this alarm (0-15).
        :param hour: Local hour (0-23).
        :param minute: Local minute (0-59).
        :param days_mask: Bitmask for days (Sun=1, Mon=2, Tue=4, Wed=8, Thu=16, Fri=32, Sat=64). 127 for daily.
        :param action_idx: The stored action index to trigger.
        """
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_DAILY_ALARM)
        
        # Original SDK logic: Bulb always operates on GMT for alarms.
        import datetime
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        local_dt = now_dt.astimezone()
        offset_seconds = (local_dt.replace(tzinfo=None) - now_dt.replace(tzinfo=None)).total_seconds()
        offset_minutes = round(offset_seconds / 60)
        
        total_minutes = (hour * 60) + minute
        gmt_total_minutes = (total_minutes - offset_minutes + 1440) % 1440
        gmt_hour = gmt_total_minutes // 60
        gmt_min = gmt_total_minutes % 60

        payload = struct.pack("<B B B B B", alarm_idx, action_idx, gmt_hour, gmt_min, days_mask)
        logger.info(f"Setting daily alarm {alarm_idx} for local {hour:02d}:{minute:02d} (GMT {gmt_hour:02d}:{gmt_min:02d})...")
        await self._send_command(header + payload)

    async def ping(self, payload: bytes = b'\xde\xad\xbe\xef', timeout: float = 5.0) -> Optional[bytes]:
        """
        Sends a ping command with an optional payload and waits for an echo response.
        :param payload: Data to be echoed back by the bulb.
        :param timeout: How long to wait for the response.
        :return: The echoed payload if successful, None otherwise.
        """
        self._last_ping_payload = None
        self._ping_event.clear()
        
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_PING)
        logger.info(f"Sending PING to {self.mac_address} with payload: {payload.hex()}")
        await self._send_command(header + payload)
        
        try:
            await asyncio.wait_for(self._ping_event.wait(), timeout=timeout)
            return self._last_ping_payload
        except asyncio.TimeoutError:
            logger.error(f"Timed out waiting for PING_ECHO from {self.mac_address}.")
            return None

    async def set_calendar_event(self, alarm_idx: int, action_idx: int, year: int, month: int, day: int, hour: int, minute: int):
        import datetime
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        local_dt = now_dt.astimezone()
        # Offset in minutes (e.g., 600 for AEST)
        offset_minutes = round((local_dt.replace(tzinfo=None) - now_dt.replace(tzinfo=None)).total_seconds() / 60)
        
        total_minutes = (hour * 60) + minute
        gmt_total_minutes = (total_minutes - offset_minutes + 1440) % 1440
        gmt_hour = gmt_total_minutes // 60
        gmt_min = gmt_total_minutes % 60

        # Conversion to BCD as expected by firmware
        bcd_hour = self._bin_to_bcd(gmt_hour)
        bcd_min = self._bin_to_bcd(gmt_min)
        
        payload = struct.pack("<B B B B B", alarm_idx, action_idx, bcd_hour, bcd_min, days_mask)
        
        logger.info(f"Setting daily alarm {alarm_idx} for local {hour:02d}:{minute:02d} (GMT {gmt_hour:02d}:{gmt_min:02d}) to trigger action {action_idx}...")
        await self._send_command(header + payload)

    async def set_calendar_event(self, alarm_idx: int, action_idx: int, year: int, month: int, day: int, hour: int, minute: int):
        """
        Schedules a one-time calendar event to trigger a stored action.
        Automatically converts local time to GMT for the bulb's internal clock.
        """
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_SET_CALENDAR_EVENT)
        
        import datetime
        local_dt = datetime.datetime(2000 + year, month, day, hour, minute)
        utc_dt = local_dt.astimezone(datetime.timezone.utc)
        
        # We use BCD for time and date fields as per firmware convention.
        bcd_year = self._bin_to_bcd(utc_dt.year % 100)
        bcd_month = self._bin_to_bcd(utc_dt.month)
        bcd_day = self._bin_to_bcd(utc_dt.day)
        bcd_hour = self._bin_to_bcd(utc_dt.hour)
        bcd_min = self._bin_to_bcd(utc_dt.minute)
        
        # Payload: alarmIdx(1), actionIdx(1), year(1), month(1), day(1), hr(1), min(1)
        payload = struct.pack("<B B B B B B B", alarm_idx, action_idx, bcd_year, bcd_month, bcd_day, bcd_hour, bcd_min)
        
        logger.info(f"Setting calendar event {alarm_idx} for {utc_dt.strftime('%Y-%m-%d %H:%M')} UTC...")
        await self._send_command(header + payload)

    async def delete_alarm(self, alarm_idx: int):
        """Deletes a scheduled alarm index."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_DELETE_ALARM)
        payload = struct.pack("<B", alarm_idx)
        logger.info(f"Deleting alarm {alarm_idx}...")
        await self._send_command(header + payload)

    async def delete_all_alarms(self):
        """Deletes all scheduled alarms on the bulb."""
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_DELETE_ALL_ALARMS)
        logger.info("Deleting all alarms...")
        await self._send_command(header)

    async def get_alarm_data(self, alarm_idx: int) -> Optional[bytes]:
        """Queries the bulb for raw alarm configuration data for a given index."""
        # For now, we return raw bytes as the response parsing is complex.
        header = self._pack_header(IlumiApiCmdType.ILUMI_API_CMD_GET_ALARM_DATA)
        payload = struct.pack("<B", alarm_idx)
        
        # Implementation of notification wait for this specific command would go here.
        # But since we don't have a specific event yet for alarm data, we'll just send it.
        # Most of our tests would rely on visual feedback or reading back status if supported.
        await self._send_command(header + payload)
        return None

async def execute_on_targets(targets: List[str], coro_func: Callable) -> Dict[str, Any]:
    """Helper to execute an SDK task across multiple bulbs."""
    results = {}
    for mac in targets:
        sdk = IlumiSDK(mac)
        try:
            await coro_func(sdk)
            results[mac] = {"success": True, "error": None}
        except Exception as e:
            logger.error(f"[{mac}] Error: {e}")
            results[mac] = {"success": False, "error": str(e)}
    return results
