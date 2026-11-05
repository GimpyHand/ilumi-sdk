"""Local web UI for controlling Ilumi bulbs via the reverse-engineered BLE SDK."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import subprocess
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import config
from effects_data import EFFECTS_DATA
from ilumi_sdk import IlumiSDK
from whites import PROFILES

ROOT = Path(__file__).resolve().parent
STATIC = ROOT / "static"
logger = logging.getLogger("ilumi.web")

DEFAULT_FAV_ID = "fav-default"
DEFAULT_FAVORITES = [
    {"id": DEFAULT_FAV_ID, "name": "default", "r": 255, "g": 140, "b": 40, "builtin": True},
    {"id": "fav-red", "name": "red", "r": 255, "g": 0, "b": 0, "builtin": True},
    {"id": "fav-orange", "name": "orange", "r": 255, "g": 122, "b": 0, "builtin": True},
    {"id": "fav-amber", "name": "amber", "r": 255, "g": 180, "b": 40, "builtin": True},
    {"id": "fav-green", "name": "green", "r": 0, "g": 255, "b": 0, "builtin": True},
    {"id": "fav-cyan", "name": "cyan", "r": 0, "g": 220, "b": 255, "builtin": True},
    {"id": "fav-blue", "name": "blue", "r": 0, "g": 80, "b": 255, "builtin": True},
    {"id": "fav-violet", "name": "violet", "r": 160, "g": 0, "b": 255, "builtin": True},
    {"id": "fav-pink", "name": "pink", "r": 255, "g": 40, "b": 160, "builtin": True},
    {"id": "fav-white", "name": "white", "r": 255, "g": 255, "b": 255, "builtin": True},
]

# Classic-app style labels for uploaded animation scenes.
EFFECT_META = {
    "trick_or_treat": {"label": "Halloween", "group": "seasonal"},
    "haunted_house": {"label": "Haunted House", "group": "seasonal"},
    "monster_mash": {"label": "Monster Mash", "group": "seasonal"},
    "jingle_bells": {"label": "Christmas", "group": "seasonal"},
    "winter_wonderland": {"label": "Winter Wonderland", "group": "seasonal"},
    "festival_of_lights": {"label": "Festival of Lights", "group": "seasonal"},
    "stars_and_stripes": {"label": "4th of July", "group": "seasonal"},
    "volcanic": {"label": "Fireplace", "group": "mood"},
    "forest": {"label": "Forest", "group": "mood"},
    "galaxy": {"label": "Galaxy", "group": "mood"},
    "ocean": {"label": "Ocean", "group": "mood"},
    "aurora": {"label": "Aurora", "group": "mood"},
    "valentine": {"label": "Valentine", "group": "mood"},
    "sweetheart": {"label": "Sweetheart", "group": "mood"},
    "fireworks": {"label": "Fireworks", "group": "party"},
    "radiation_leak": {"label": "Radiation Leak", "group": "party"},
    "year_of_the_rooster": {"label": "Rooster", "group": "party"},
}


def _effect_catalog() -> list[dict]:
    catalog = []
    for key in EFFECTS_DATA:
        meta = EFFECT_META.get(key, {})
        colors = _palette_from_theme(key)[:5]
        catalog.append(
            {
                "id": key,
                "label": meta.get("label") or key.replace("_", " ").title(),
                "group": meta.get("group", "mood"),
                "colors": colors,
            }
        )
    return catalog

app = FastAPI(title="ilumi", docs_url=None, redoc_url=None)

# Wire order is RGB (matches PROTOCOL / official SDK packing).
# An earlier RBG remap made green render as blue — do not reintroduce it.
def to_wire(r: int, g: int, b: int) -> tuple[int, int, int]:
    return (r, g, b)


def to_rgbw(r: int, g: int, b: int, w: int = 0) -> tuple[int, int, int, int]:
    """Map sRGB intent to Ilumi RGBW.

    Equal RGB on the color LEDs alone looks magenta; white must go on W.
    When the caller already supplies W (profiles / advanced), pass through.
    Otherwise extract white as min(R,G,B) and leave only the residual on RGB.
    """
    if w:
        wr, wg, wb = to_wire(r, g, b)
        return wr, wg, wb, w
    ww = min(r, g, b)
    wr, wg, wb = to_wire(r - ww, g - ww, b - ww)
    return wr, wg, wb, ww


class BulbSession:
    """Keeps one live BLE connection so color updates stay fast."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._sdk: Optional[IlumiSDK] = None
        self._mac: Optional[str] = None
        self._last_used = 0.0
        self._idle_task: Optional[asyncio.Task] = None

    @property
    def connected(self) -> bool:
        return bool(self._sdk and self._sdk.is_connected)

    async def close(self) -> None:
        if self._idle_task and not self._idle_task.done():
            self._idle_task.cancel()
        sdk, self._sdk, self._mac = self._sdk, None, None
        if sdk is not None:
            try:
                await sdk.__aexit__(None, None, None)
            except Exception:
                pass

    def _bump_idle(self) -> None:
        self._last_used = time.monotonic()
        if self._idle_task and not self._idle_task.done():
            self._idle_task.cancel()
        self._idle_task = asyncio.create_task(self._idle_watch())

    async def _idle_watch(self) -> None:
        try:
            # Keep the link warm — reconnects are what make power feel broken.
            await asyncio.sleep(300)
            if time.monotonic() - self._last_used >= 295:
                async with self._lock:
                    await self.close()
        except asyncio.CancelledError:
            return

    async def ensure(self, mac: str) -> IlumiSDK:
        mac = config.normalize_mac(mac) or mac
        if (
            self._sdk
            and self._sdk.is_connected
            and config.normalize_mac(self._mac) == mac
        ):
            self._bump_idle()
            return self._sdk

        await self.close()
        last_err: Exception | None = None
        for attempt in range(4):
            sdk = IlumiSDK(mac)
            try:
                await sdk.__aenter__()
                self._sdk = sdk
                self._mac = mac
                self._bump_idle()
                return sdk
            except Exception as exc:
                last_err = exc
                try:
                    await sdk.__aexit__(None, None, None)
                except Exception:
                    pass
                await asyncio.sleep(0.15 * (attempt + 1))
        raise last_err  # type: ignore[misc]

    async def run(self, mac: str, action):
        async with self._lock:
            sdk = await self.ensure(mac)
            try:
                return await action(sdk)
            except Exception:
                # Drop dead session so the next call reconnects cleanly.
                await self.close()
                raise


session = BulbSession()


def _default_monitor_source() -> str:
    """PipeWire/Pulse monitor of the current default sink (system playback)."""
    try:
        sink = subprocess.check_output(
            ["pactl", "get-default-sink"], text=True, timeout=2
        ).strip()
    except Exception as exc:
        raise RuntimeError(f"Could not resolve default sink: {exc}") from exc
    if not sink:
        raise RuntimeError("No default audio sink")
    return f"{sink}.monitor"


def _palette_from_theme(theme: str) -> list[dict]:
    """Unique RGB colors from a classic animated scene."""
    key = theme.lower().replace(" ", "_")
    frames = EFFECTS_DATA.get(key) or []
    seen: set[tuple[int, int, int]] = set()
    out: list[dict] = []
    for f in frames:
        r, g, b = int(f.get("r", 0)), int(f.get("g", 0)), int(f.get("b", 0))
        w = int(f.get("w", 0))
        # Pure-W frames (common in holiday scenes) should still count as white.
        if (r, g, b) == (0, 0, 0) and w:
            r = g = b = min(255, max(w, 200))
        rgb = (r, g, b)
        if rgb == (0, 0, 0) or rgb in seen:
            continue
        seen.add(rgb)
        out.append({"r": rgb[0], "g": rgb[1], "b": rgb[2]})
    return out


def _normalize_palette(colors: list) -> list[dict]:
    out: list[dict] = []
    seen: set[tuple[int, int, int]] = set()
    for c in colors or []:
        if isinstance(c, dict):
            r, g, b = int(c.get("r", 0)), int(c.get("g", 0)), int(c.get("b", 0))
        elif isinstance(c, (list, tuple)) and len(c) >= 3:
            r, g, b = int(c[0]), int(c[1]), int(c[2])
        else:
            continue
        r, g, b = max(0, min(255, r)), max(0, min(255, g)), max(0, min(255, b))
        key = (r, g, b)
        if key in seen:
            continue
        seen.add(key)
        out.append({"r": r, "g": g, "b": b})
    return out or [{"r": 255, "g": 80, "b": 0}, {"r": 0, "g": 180, "b": 255}, {"r": 255, "g": 255, "b": 255}]


class SystemBeat:
    """Advance through a chosen color palette on beat / timer triggers."""

    SAMPLE_RATE = 48000
    BLOCK = 2048
    BASS_MIN, BASS_MAX = 20, 180
    # Middle ground: reacts to real kicks without the earlier ~2 Hz flicker.
    ONSET_COOLDOWN = 0.38

    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._stream = None
        self.mac: Optional[str] = None
        self.monitor: Optional[str] = None
        self.theme: Optional[str] = None
        self.shuffle = False
        self.trigger = "beat"  # beat | client | interval | random
        self.interval = 2.0
        self.brightness = 220
        self.palette: list[dict] = _normalize_palette([])
        self.index = 0
        self.energy = 0.0
        self.r = self.palette[0]["r"]
        self.g = self.palette[0]["g"]
        self.b = self.palette[0]["b"]
        self._env = 0.0
        self._noise = 0.02
        self._prev_rms = 0.0
        self._flux_avg = 0.0
        self._bass_env = 0.0
        self._last_onset = 0.0
        self._next_tick = 0.0
        self._pending_advance = False
        self._dirty = True
        self._pulse = 0.7
        self._lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        return bool(self._task and not self._task.done())

    def status(self) -> dict:
        return {
            "running": self.running,
            "mac": self.mac,
            "monitor": self.monitor,
            "theme": self.theme,
            "shuffle": self.shuffle,
            "trigger": self.trigger,
            "interval": self.interval,
            "brightness": self.brightness,
            "palette": list(self.palette),
            "index": self.index,
            "energy": round(self.energy, 3),
            "color": {"r": self.r, "g": self.g, "b": self.b},
        }

    def set_palette(
        self,
        colors: Optional[list] = None,
        theme: Optional[str] = None,
        shuffle: Optional[bool] = None,
        trigger: Optional[str] = None,
        interval: Optional[float] = None,
        brightness: Optional[int] = None,
    ) -> None:
        if theme:
            extracted = _palette_from_theme(theme)
            if extracted:
                self.theme = theme.lower().replace(" ", "_")
                self.palette = extracted
            else:
                self.theme = None
                self.palette = _normalize_palette(colors or self.palette)
        elif colors is not None:
            self.theme = None
            self.palette = _normalize_palette(colors)
        if shuffle is not None:
            self.shuffle = bool(shuffle)
        if trigger is not None:
            t = str(trigger).lower().strip()
            self.trigger = t if t in ("beat", "client", "interval", "random") else "beat"
        if interval is not None:
            self.interval = max(0.08, min(30.0, float(interval)))
        if brightness is not None:
            self.brightness = max(8, min(255, int(brightness)))
        # Prefer a color different from whatever is currently showing so a
        # mode tap always looks like a change, even before the next beat.
        prev = (self.r, self.g, self.b)
        self.index = 0
        for i, c in enumerate(self.palette):
            if (c["r"], c["g"], c["b"]) != prev:
                self.index = i
                break
        c = self.palette[self.index]
        self.r, self.g, self.b = c["r"], c["g"], c["b"]
        self._dirty = True
        self._pending_advance = False
        self._next_tick = time.monotonic() + self.interval

    def _advance(self) -> None:
        if self.shuffle and len(self.palette) > 1:
            nxt = self.index
            while nxt == self.index:
                nxt = random.randrange(len(self.palette))
            self.index = nxt
        else:
            self.index = (self.index + 1) % len(self.palette)
        c = self.palette[self.index]
        self.r, self.g, self.b = c["r"], c["g"], c["b"]
        self._dirty = True

    def _on_audio(self, indata, frames, time_info, status) -> None:  # noqa: ARG002
        import numpy as np

        if self.trigger != "beat":
            return
        if status:
            logger.debug("audio status: %s", status)
        mono = np.mean(indata, axis=1).astype(np.float64)
        rms = float(np.sqrt(np.mean(mono * mono)) + 1e-9)
        fft = np.abs(np.fft.rfft(mono))
        freqs = np.fft.rfftfreq(len(mono), 1.0 / self.SAMPLE_RATE)
        bass_idx = np.where((freqs >= self.BASS_MIN) & (freqs <= self.BASS_MAX))[0]
        bass = float(np.mean(fft[bass_idx])) if len(bass_idx) else 0.0

        # Slow noise floor + faster local envelope so kicks stand out from
        # continuous music (desktop playback often sits around energy 0.2–0.4).
        self._noise = self._noise * 0.98 + max(rms, bass * 0.0015) * 0.02
        floor = max(self._noise, 1e-5)
        level = min(1.0, rms / (floor * 2.4))
        bass_n = min(1.0, bass / (floor * 80.0 + 1e-6))
        flux = max(0.0, rms - self._prev_rms)
        self._prev_rms = rms
        self._flux_avg = self._flux_avg * 0.92 + flux * 0.08
        self._env = self._env * 0.82 + level * 0.18
        self._bass_env = self._bass_env * 0.85 + bass_n * 0.15
        self.energy = float(0.65 * self._env + 0.35 * self._bass_env)
        # Mild brightness breath so the bulb always “moves” with the music.
        self._pulse = max(0.45, min(1.0, 0.5 * self._pulse + 0.5 * (0.55 + 0.45 * self.energy)))

        now = time.monotonic()
        flux_hit = flux > max(self._flux_avg * 2.4, floor * 0.45)
        level_hit = level > max(0.22, self._env * 1.28)
        bass_hit = bass_n > max(0.28, self._bass_env * 1.35)
        onset = (
            (now - self._last_onset) > self.ONSET_COOLDOWN
            and level > 0.16
            and ((flux_hit and level_hit) or (bass_hit and level_hit) or (flux_hit and bass_hit))
        )
        if onset:
            self._last_onset = now
            self._pending_advance = True
            self._pulse = 1.0

    def pulse(self, energy: Optional[float] = None) -> dict:
        """Advance from a browser-mic onset (phone music → this device's mic)."""
        now = time.monotonic()
        if (now - self._last_onset) < self.ONSET_COOLDOWN:
            return self.status()
        self._last_onset = now
        self._pending_advance = True
        self._pulse = 1.0
        if energy is None:
            self.energy = 1.0
        else:
            self.energy = max(0.0, min(1.0, float(energy)))
        return self.status()

    async def start(
        self,
        mac: str,
        colors: Optional[list] = None,
        theme: Optional[str] = None,
        shuffle: bool = False,
        trigger: str = "beat",
        interval: float = 2.0,
        brightness: int = 220,
    ) -> dict:
        async with self._lock:
            await self._stop_unlocked()
            self.mac = config.normalize_mac(mac) or mac
            self.monitor = None
            self._env = 0.0
            self._noise = 0.02
            self._prev_rms = 0.0
            self._flux_avg = 0.0
            self._bass_env = 0.0
            self._pulse = 0.75
            self._last_onset = 0.0
            self.set_palette(
                colors=colors,
                theme=theme,
                shuffle=shuffle,
                trigger=trigger,
                interval=interval,
                brightness=brightness,
            )
            self._dirty = True

            if self.trigger == "beat":
                monitor = _default_monitor_source()
                self.monitor = monitor
                import sounddevice as sd

                os.environ["PULSE_SOURCE"] = monitor
                pulse_idx = next(
                    (
                        i
                        for i, d in enumerate(sd.query_devices())
                        if d["name"] == "pulse" and d["max_input_channels"] > 0
                    ),
                    None,
                )
                if pulse_idx is None:
                    raise RuntimeError("Pulse audio device not available")

                self._stream = sd.InputStream(
                    device=pulse_idx,
                    channels=2,
                    samplerate=self.SAMPLE_RATE,
                    blocksize=self.BLOCK,
                    callback=self._on_audio,
                )
                self._stream.start()

            self._task = asyncio.create_task(self._drive(self.mac))
            logger.info(
                "Palette runner on %s (%d colors, trigger=%s%s)",
                self.mac,
                len(self.palette),
                self.trigger,
                f", theme={self.theme}" if self.theme else "",
            )
            return self.status()

    async def stop(self) -> dict:
        async with self._lock:
            await self._stop_unlocked()
            return self.status()

    async def _stop_unlocked(self) -> None:
        task, self._task = self._task, None
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        stream, self._stream = self._stream, None
        if stream is not None:
            try:
                stream.stop()
                stream.close()
            except Exception:
                pass
        self.energy = 0.0

    async def _drive(self, mac: str) -> None:
        last_sent = (-1, -1, -1, -1)
        try:
            while True:
                now = time.monotonic()
                if self.trigger == "interval" and now >= self._next_tick:
                    self._pending_advance = True
                    self._next_tick = now + self.interval
                elif self.trigger == "random" and now >= self._next_tick:
                    self._pending_advance = True
                    jitter = self.interval * random.uniform(0.45, 1.6)
                    self._next_tick = now + max(0.4, jitter)

                if self._pending_advance:
                    self._pending_advance = False
                    self._advance()
                r, g, b = self.r, self.g, self.b
                # Colour steps on onsets; brightness gently follows energy.
                pulse = getattr(self, "_pulse", 0.75)
                bri = max(28, min(255, int(self.brightness * pulse)))
                bri_q = (bri // 10) * 10  # avoid spamming near-identical writes
                key = (r, g, b, bri_q)
                if not self._dirty and key == last_sent:
                    await asyncio.sleep(0.05)
                    continue
                try:

                    async def action(sdk: IlumiSDK, rr=r, gg=g, bb=b, brightness=bri_q):
                        wr, wg, wb, ww = to_rgbw(rr, gg, bb, 0)
                        await sdk.set_color_fast(wr, wg, wb, ww, brightness)

                    await session.run(mac, action)
                    last_sent = key
                    self._dirty = False
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("beat color send failed: %s", exc)
                    await asyncio.sleep(0.35)
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        finally:
            if self._stream is not None:
                try:
                    self._stream.stop()
                    self._stream.close()
                except Exception:
                    pass
                self._stream = None


beat = SystemBeat()


class EnrollRequest(BaseModel):
    address: str
    name: str = Field(min_length=1, max_length=64)
    groups: str = ""


class ColorRequest(BaseModel):
    r: int = Field(ge=0, le=255)
    g: int = Field(ge=0, le=255)
    b: int = Field(ge=0, le=255)
    w: int = Field(default=0, ge=0, le=255)
    brightness: int = Field(default=255, ge=0, le=255)
    fade_ms: int = Field(default=0, ge=0, le=10000)


class WhiteRequest(BaseModel):
    profile: str
    brightness: int = Field(default=255, ge=0, le=255)
    fade_ms: int = Field(default=0, ge=0, le=10000)


class PowerRequest(BaseModel):
    fade_ms: int = Field(default=200, ge=0, le=10000)


class EffectRequest(BaseModel):
    effect: str = Field(min_length=1, max_length=64)


class CandleRequest(BaseModel):
    r: int = Field(default=255, ge=0, le=255)
    g: int = Field(default=150, ge=0, le=255)
    b: int = Field(default=0, ge=0, le=255)
    w: int = Field(default=29, ge=0, le=255)
    brightness: int = Field(default=255, ge=0, le=255)


class RandomRequest(BaseModel):
    sequence: bool = False


class FavoriteCreate(BaseModel):
    name: str = Field(default="", max_length=32)
    r: int = Field(ge=0, le=255)
    g: int = Field(ge=0, le=255)
    b: int = Field(ge=0, le=255)


class BeatColor(BaseModel):
    r: int = Field(ge=0, le=255)
    g: int = Field(ge=0, le=255)
    b: int = Field(ge=0, le=255)


class BeatRequest(BaseModel):
    palette: list[BeatColor] = Field(default_factory=list)
    theme: Optional[str] = None
    shuffle: bool = False
    trigger: str = "beat"
    interval: float = Field(default=2.0, ge=0.08, le=30.0)
    brightness: int = Field(default=220, ge=8, le=255)


class BeatPulse(BaseModel):
    energy: float = Field(default=1.0, ge=0.0, le=1.0)


def _default_favorite() -> dict:
    return dict(next(f for f in DEFAULT_FAVORITES if f["id"] == DEFAULT_FAV_ID))


def _favorites() -> list[dict]:
    stored = config.get_config("favorites")
    items = list(stored) if isinstance(stored, list) and stored else list(DEFAULT_FAVORITES)
    if not any(f.get("id") == DEFAULT_FAV_ID for f in items):
        items.insert(0, _default_favorite())
    return items


def _save_favorites(items: list[dict]) -> None:
    config.update_config("favorites", items)


def _default_bulb_state() -> dict:
    fav = _default_favorite()
    return {
        "power": False,
        "r": fav["r"],
        "g": fav["g"],
        "b": fav["b"],
        "w": 0,
        "brightness": 255,
        "white": None,
        "effect": None,
        "name": fav["name"],
        "style": "party",
        "shuffle": False,
        "palette_ids": [DEFAULT_FAV_ID],
    }


def _bulb_state_key(mac: str) -> str:
    bulbs = config.get_all_bulbs()
    norm = config.normalize_mac(mac)
    for stored_mac in bulbs:
        if config.normalize_mac(stored_mac) == norm:
            return stored_mac
    return norm or mac


def _get_bulb_state(mac: str) -> dict:
    all_states = config.get_config("bulb_state") or {}
    if not isinstance(all_states, dict):
        all_states = {}
    norm = config.normalize_mac(mac)
    for key, value in all_states.items():
        if config.normalize_mac(key) == norm and isinstance(value, dict):
            merged = _default_bulb_state()
            merged.update(value)
            if not merged.get("palette_ids"):
                merged["palette_ids"] = [DEFAULT_FAV_ID]
            return merged
    return _default_bulb_state()


def _save_bulb_state(mac: str, patch: dict) -> dict:
    key = _bulb_state_key(mac)
    all_states = config.get_config("bulb_state") or {}
    if not isinstance(all_states, dict):
        all_states = {}
    cur = _get_bulb_state(mac)
    for field, value in patch.items():
        cur[field] = value
    all_states[key] = cur
    config.update_config("bulb_state", all_states)
    return cur


async def _apply_saved_look(sdk: IlumiSDK, st: dict) -> None:
    """Push remembered colour / white onto the bulb."""
    bri = int(st.get("brightness") or 255)
    white = st.get("white")
    if white and white in PROFILES:
        r, g, b, w = PROFILES[white]
        wr, wg, wb = to_wire(r, g, b)
        await sdk.set_color_fast(wr, wg, wb, w, bri)
        return
    wr, wg, wb, ww = to_rgbw(
        int(st.get("r", 255)),
        int(st.get("g", 140)),
        int(st.get("b", 40)),
        int(st.get("w", 0) or 0),
    )
    await sdk.set_color_fast(wr, wg, wb, ww, bri)


def _bulb_payload(mac: str, data: dict) -> dict:
    return {
        "mac": mac,
        "name": data.get("name", mac),
        "groups": data.get("groups") or ([data["group"]] if data.get("group") else []),
        "node_id": data.get("node_id"),
    }


def _require_bulb(mac: str) -> dict:
    bulbs = config.get_all_bulbs()
    norm = config.normalize_mac(mac)
    for stored_mac, data in bulbs.items():
        if config.normalize_mac(stored_mac) == norm:
            return {"mac": stored_mac, **data}
    raise HTTPException(status_code=404, detail=f"Bulb {mac} is not enrolled")


@app.on_event("shutdown")
async def _shutdown():
    await beat.stop()
    await session.close()


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/favicon.ico")
async def favicon_ico():
    return FileResponse(STATIC / "favicon.ico", media_type="image/x-icon")


@app.get("/apple-touch-icon.png")
@app.get("/apple-touch-icon-precomposed.png")
@app.get("/apple-touch-icon-120x120.png")
@app.get("/apple-touch-icon-120x120-precomposed.png")
@app.get("/apple-touch-icon-152x152.png")
@app.get("/apple-touch-icon-152x152-precomposed.png")
@app.get("/apple-touch-icon-167x167.png")
@app.get("/apple-touch-icon-167x167-precomposed.png")
@app.get("/apple-touch-icon-180x180.png")
@app.get("/apple-touch-icon-180x180-precomposed.png")
@app.get("/apple-touch-icon-240x240.png")
@app.get("/apple-touch-icon-240x240-precomposed.png")
async def apple_touch_icon():
    return FileResponse(STATIC / "apple-touch-icon.png", media_type="image/png")


@app.get("/manifest.webmanifest")
async def web_manifest():
    return FileResponse(STATIC / "manifest.webmanifest", media_type="application/manifest+json")


@app.get("/api/status")
async def status():
    bulbs = [_bulb_payload(mac, data) for mac, data in config.get_all_bulbs().items()]
    states = {mac: _get_bulb_state(mac) for mac, _ in config.get_all_bulbs().items()}
    return {
        "bulbs": bulbs,
        "network_key": config.get_config("network_key"),
        "whites": list(PROFILES.keys()),
        "effects": _effect_catalog(),
        "favorites": _favorites(),
        "states": states,
        "default_favorite_id": DEFAULT_FAV_ID,
        "beat": beat.status(),
        "ble_connected": session.connected,
        "wire_order": "RGB",
    }


@app.get("/api/favorites")
async def list_favorites():
    return {"favorites": _favorites()}


@app.post("/api/favorites")
async def create_favorite(body: FavoriteCreate):
    items = _favorites()
    name = (body.name or "").strip() or f"#{body.r:02x}{body.g:02x}{body.b:02x}"
    fav = {
        "id": f"fav-{int(time.time() * 1000)}-{random.randint(100, 999)}",
        "name": name[:32],
        "r": body.r,
        "g": body.g,
        "b": body.b,
        "builtin": False,
    }
    items.append(fav)
    _save_favorites(items)
    return {"ok": True, "favorite": fav, "favorites": items}


@app.delete("/api/favorites/{fav_id}")
async def delete_favorite(fav_id: str):
    items = [f for f in _favorites() if f.get("id") != fav_id]
    if not items:
        items = list(DEFAULT_FAVORITES)
    _save_favorites(items)
    return {"ok": True, "favorites": items}


@app.post("/api/scan")
async def scan(timeout: float = 6.0):
    timeout = max(2.0, min(timeout, 20.0))
    await session.close()
    found = await IlumiSDK.discover(timeout=timeout)
    enrolled = {
        config.normalize_mac(mac): data for mac, data in config.get_all_bulbs().items()
    }
    results = []
    for bulb in found:
        norm = config.normalize_mac(bulb["address"])
        existing = enrolled.get(norm)
        results.append(
            {
                **bulb,
                "enrolled": existing is not None,
                "enrolled_name": existing.get("name") if existing else None,
            }
        )
    return {"bulbs": results}


@app.post("/api/enroll")
async def enroll(body: EnrollRequest):
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Name is required")

    network_key = config.get_config("network_key")
    if not network_key:
        network_key = random.randint(1000, 999999)
        config.update_config("network_key", network_key)

    await session.close()

    async def action(sdk: IlumiSDK):
        wr, wg, wb = to_wire(0, 255, 0)
        await sdk.set_color_fast(wr, wg, wb, 0, 255)
        await asyncio.sleep(0.25)
        node_id = config.add_bulb(body.address, name, body.groups)
        ok = await sdk.commission(network_key, 1, node_id)
        if not ok:
            raise HTTPException(status_code=502, detail="Commission failed")
        wr, wg, wb = to_wire(0, 100, 255)
        await sdk.set_color_fast(wr, wg, wb, 0, 255)
        await asyncio.sleep(0.25)
        await sdk.set_color_fast(0, 0, 0, 0, 0)
        return node_id

    try:
        await session.run(body.address, action)
        await session.close()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    bulb = _require_bulb(body.address)
    return {"ok": True, "bulb": _bulb_payload(bulb["mac"], bulb)}


class RememberRequest(BaseModel):
    palette_ids: Optional[list[str]] = None
    style: Optional[str] = None
    shuffle: Optional[bool] = None
    name: Optional[str] = None
    power: Optional[bool] = None


@app.post("/api/bulbs/{mac}/on")
async def turn_on(mac: str, body: PowerRequest = PowerRequest()):
    bulb = _require_bulb(mac)
    # Beat owns the BLE link while running — stop it so power isn't starved.
    await beat.stop()
    st = _get_bulb_state(bulb["mac"])

    async def action(sdk: IlumiSDK):
        # Instant path: short fade, then restore last app settings.
        await sdk.turn_on(transit=min(body.fade_ms, 250))
        await _apply_saved_look(sdk, st)

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    saved = _save_bulb_state(bulb["mac"], {"power": True})
    return {"ok": True, "power": "on", "state": saved, "beat": beat.status()}


@app.post("/api/bulbs/{mac}/off")
async def turn_off(mac: str, body: PowerRequest = PowerRequest()):
    bulb = _require_bulb(mac)
    await beat.stop()
    # Keep last look on disk for the next app On; write the default colour
    # into the bulb so a hard power-cycle comes back to default.
    home = _default_favorite()
    saved = _save_bulb_state(bulb["mac"], {"power": False})

    async def action(sdk: IlumiSDK):
        wr, wg, wb, ww = to_rgbw(home["r"], home["g"], home["b"], 0)
        await sdk.set_color_fast(wr, wg, wb, ww, int(saved.get("brightness") or 255))
        await sdk.turn_off(transit=min(body.fade_ms, 250))

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"ok": True, "power": "off", "state": saved, "beat": beat.status()}


@app.post("/api/bulbs/{mac}/color")
async def set_color(mac: str, body: ColorRequest):
    bulb = _require_bulb(mac)
    if beat.running:
        await beat.stop()
    wr, wg, wb, ww = to_rgbw(body.r, body.g, body.b, body.w)

    async def action(sdk: IlumiSDK):
        # Fast write-without-response; fade only when explicitly requested.
        if body.fade_ms > 0:
            await sdk.set_color_smooth(
                wr, wg, wb, ww, body.brightness, duration_ms=body.fade_ms
            )
        else:
            await sdk.set_color_fast(wr, wg, wb, ww, body.brightness)

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    # Match a named favourite when possible so the UI can show a label.
    matched = next(
        (
            f
            for f in _favorites()
            if f.get("r") == body.r and f.get("g") == body.g and f.get("b") == body.b
        ),
        None,
    )
    saved = _save_bulb_state(
        bulb["mac"],
        {
            "power": True,
            "r": body.r,
            "g": body.g,
            "b": body.b,
            "w": body.w,
            "brightness": body.brightness,
            "white": None,
            "effect": None,
            "name": matched["name"] if matched else "",
        },
    )
    return {
        "ok": True,
        "color": body.model_dump(),
        "wire": {"r": wr, "g": wg, "b": wb, "w": ww},
        "state": saved,
    }


@app.post("/api/bulbs/{mac}/white")
async def set_white(mac: str, body: WhiteRequest):
    bulb = _require_bulb(mac)
    key = body.profile.lower().replace(" ", "_")
    if key not in PROFILES:
        raise HTTPException(status_code=400, detail=f"Unknown profile: {body.profile}")
    r, g, b, w = PROFILES[key]
    wr, wg, wb = to_wire(r, g, b)

    async def action(sdk: IlumiSDK):
        if body.fade_ms > 0:
            await sdk.set_color_smooth(
                wr, wg, wb, w, body.brightness, duration_ms=body.fade_ms
            )
        else:
            await sdk.set_color_fast(wr, wg, wb, w, body.brightness)

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    saved = _save_bulb_state(
        bulb["mac"],
        {
            "power": True,
            "r": r,
            "g": g,
            "b": b,
            "w": w,
            "brightness": body.brightness,
            "white": key,
            "effect": None,
            "name": key.replace("_", " "),
        },
    )
    return {"ok": True, "profile": key, "rgbw": [r, g, b, w], "state": saved}


@app.post("/api/bulbs/{mac}/remember")
async def remember_state(mac: str, body: RememberRequest):
    """Persist UI-only settings (palette selection, tempo) without touching BLE."""
    bulb = _require_bulb(mac)
    patch: dict[str, Any] = {}
    if body.palette_ids is not None:
        patch["palette_ids"] = body.palette_ids or [DEFAULT_FAV_ID]
    if body.style is not None:
        patch["style"] = body.style
    if body.shuffle is not None:
        patch["shuffle"] = body.shuffle
    if body.name is not None:
        patch["name"] = body.name
    if body.power is not None:
        patch["power"] = body.power
    saved = _save_bulb_state(bulb["mac"], patch) if patch else _get_bulb_state(bulb["mac"])
    return {"ok": True, "state": saved}


@app.post("/api/bulbs/{mac}/warmup")
async def warmup(mac: str):
    """Pre-connect and re-apply last look if the bulb was left on (server recovery)."""
    bulb = _require_bulb(mac)
    st = _get_bulb_state(bulb["mac"])

    async def action(sdk: IlumiSDK):
        if st.get("power"):
            await sdk.turn_on(transit=0)
            await _apply_saved_look(sdk, st)

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"ok": True, "ble_connected": True, "state": st}


@app.post("/api/bulbs/{mac}/effect")
async def play_effect(mac: str, body: EffectRequest):
    """Upload + start a classic animated color pattern on the bulb."""
    bulb = _require_bulb(mac)
    await beat.stop()
    key = body.effect.lower().replace(" ", "_")
    frames = EFFECTS_DATA.get(key)
    if not frames:
        raise HTTPException(status_code=400, detail=f"Unknown effect: {body.effect}")
    scene_idx = 4

    async def action(sdk: IlumiSDK):
        await sdk.set_color_pattern(scene_idx, frames, repeatable=255, start_now=1)
        await sdk.start_color_pattern(scene_idx)

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    meta = next((e for e in _effect_catalog() if e["id"] == key), {"id": key, "label": key})
    saved = _save_bulb_state(
        bulb["mac"],
        {"power": True, "effect": key, "white": None, "name": meta.get("label") or key},
    )
    return {"ok": True, "effect": meta, "state": saved}


@app.post("/api/bulbs/{mac}/candle")
async def candle_mode(mac: str, body: CandleRequest = CandleRequest()):
    bulb = _require_bulb(mac)
    await beat.stop()
    wr, wg, wb, ww = to_rgbw(body.r, body.g, body.b, body.w)

    async def action(sdk: IlumiSDK):
        await sdk.set_candle_mode(wr, wg, wb, ww, body.brightness)

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"ok": True, "mode": "candle"}


@app.post("/api/bulbs/{mac}/random")
async def random_mode(mac: str, body: RandomRequest = RandomRequest()):
    bulb = _require_bulb(mac)
    await beat.stop()

    async def action(sdk: IlumiSDK):
        if body.sequence:
            await sdk.random_color_sequence()
        else:
            await sdk.set_random_color()

    try:
        await session.run(bulb["mac"], action)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"ok": True, "mode": "random_sequence" if body.sequence else "random_once"}


@app.get("/api/beat")
async def beat_status():
    return beat.status()


@app.post("/api/bulbs/{mac}/beat/start")
async def beat_start(mac: str, body: BeatRequest = BeatRequest()):
    bulb = _require_bulb(mac)
    colors = [c.model_dump() for c in body.palette] if body.palette else [
        {"r": f["r"], "g": f["g"], "b": f["b"]} for f in _favorites()
    ]
    try:
        status = await beat.start(
            bulb["mac"],
            colors=colors,
            theme=body.theme,
            shuffle=body.shuffle,
            trigger=body.trigger,
            interval=body.interval,
            brightness=body.brightness,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"ok": True, "beat": status}


@app.post("/api/bulbs/{mac}/beat/stop")
async def beat_stop(mac: str):
    _require_bulb(mac)
    status = await beat.stop()
    return {"ok": True, "beat": status}


@app.post("/api/beat/pulse")
async def beat_pulse(body: BeatPulse = BeatPulse()):
    if not beat.running:
        raise HTTPException(status_code=409, detail="Beat is not running")
    if beat.trigger != "client":
        raise HTTPException(status_code=409, detail="Beat is not in client mode")
    return {"ok": True, "beat": beat.pulse(body.energy)}


@app.post("/api/beat/palette")
async def beat_palette(body: BeatRequest):
    if not beat.running:
        raise HTTPException(status_code=409, detail="Beat is not running")
    colors = [c.model_dump() for c in body.palette] if body.palette else None
    beat.set_palette(
        colors=colors,
        theme=body.theme,
        shuffle=body.shuffle,
        trigger=body.trigger,
        interval=body.interval,
        brightness=body.brightness,
    )
    return {"ok": True, "beat": beat.status()}


app.mount("/static", StaticFiles(directory=STATIC), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("webapp:app", host="0.0.0.0", port=8765, reload=False)
