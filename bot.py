# -*- coding: utf-8 -*-
# ============================================================================
#  🎬 Sinhala ASS Karaoke Burner Bot  —  Google Colab (T4 GPU / NVENC)
#  මේ සම්පූර්ණ Code එක Colab Cell එකකට Paste කරල Run කරන්න.
#  Runtime -> Change runtime type -> T4 GPU තෝරන්න.
# ============================================================================
import os, sys, subprocess, shutil, time

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID = int(os.getenv("API_ID", ))
API_HASH = os.getenv("API_HASH", "")

WORK_ROOT = "/content/work" if os.path.isdir("/content") else os.path.abspath("work")
FONT_DIR = os.path.expanduser("~/.fonts")


def log(msg):
    print(msg, flush=True)


def sh(cmd, quiet=True):
    r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0 and not quiet:
        log(r.stdout[-800:])
    return r


# ----------------------------------------------------------------------------
#  1) AUTO INSTALL
# ----------------------------------------------------------------------------
def patch_font(path):
    """Font එකේ U+25CC (◌ Dotted circle) Glyph එක ඉවත් කරනවා.
    HarfBuzz ට තනි පිල්ලමකට ◌ එකතු කරන්න වෙන්නෙ Font එකේ ඒ Glyph එක තියෙනවා නම් විතරයි.
    (Windows වල Fonts වල ඒක නැති නිසා ඒවායේ ◌ පෙනෙන්නෙ නෑ.)"""
    try:
        from fontTools.ttLib import TTFont, TTCollection
        is_ttc = path.lower().endswith(".ttc")
        obj = TTCollection(path, lazy=False) if is_ttc else TTFont(path, lazy=False)
        changed = False
        for f in (obj.fonts if is_ttc else [obj]):
            cm = f.getBestCmap() or {}
            if 0x0D9A in cm and 0x25CC in cm:
                for t in f["cmap"].tables:
                    t.cmap.pop(0x25CC, None)
                changed = True
        if changed:
            tmp = path + ".tmp"
            obj.save(tmp)
            os.replace(tmp, path)
        return changed
    except Exception as e:
        log(f"      ⚠️  Font Patch අසාර්ථකයි ({os.path.basename(path)}): {e}")
        return False


def extract_embedded_fonts(video, dest):
    """වීඩියෝව (MKV) ඇතුළේ Attachment විදිහට තියෙන Fonts `dest` එකට Extract කරලා,
    ඒවායේ Family නම් -> {"si": සිංහල Glyphs තියෙනවද, "latin": English Glyphs තියෙනවද} Dict එකක් දෙනවා."""
    os.makedirs(dest, exist_ok=True)
    try:
        r = subprocess.run(["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", video],
                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=60)
        streams = json.loads(r.stdout or b"{}").get("streams", [])
        atts = [s for s in streams if s.get("codec_type") == "attachment"]
        if not atts:
            return {}
        # ගොනු නම් අපි තීරණය කරනවා (Video එකේ Filename Tag එක විශ්වාස කරන්නෙ නෑ)
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]
        for n in range(len(atts)):
            cmd += [f"-dump_attachment:t:{n}", os.path.join(dest, f"emb_{n}.bin")]
        cmd += ["-i", video, "-t", "0", "-f", "null", "-"]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
    except Exception as e:
        log(f"      ⚠️  Embedded Fonts Extract අසාර්ථකයි: {e}")
        return {}

    from fontTools.ttLib import TTFont, TTCollection
    fams = {}
    for fn in sorted(os.listdir(dest)):
        if not fn.startswith("emb_") or not fn.endswith(".bin"):
            continue
        p = os.path.join(dest, fn)
        try:
            with open(p, "rb") as fh:
                magic = fh.read(4)
            ext = {b"OTTO": ".otf", b"ttcf": ".ttc", b"\x00\x01\x00\x00": ".ttf", b"true": ".ttf"}.get(magic)
            if not ext or os.path.getsize(p) > 30 * 1024 * 1024:
                os.remove(p)       # Font එකක් නෙවෙයි
                continue
            np_ = p[:-4] + ext
            os.replace(p, np_)
            patch_font(np_)
            obj = TTCollection(np_, lazy=True) if ext == ".ttc" else TTFont(np_, lazy=True)
            for f in (obj.fonts if ext == ".ttc" else [obj]):
                cm = f.getBestCmap() or {}
                info = {"si": 0x0D9A in cm, "latin": 0x41 in cm and 0x61 in cm}
                for rec in f["name"].names:
                    if rec.nameID in (1, 4, 6, 16):
                        try:
                            nm = rec.toUnicode().strip().lower()
                        except Exception:
                            continue
                        if nm:
                            cur = fams.setdefault(nm, {"si": False, "latin": False})
                            cur["si"] |= info["si"]
                            cur["latin"] |= info["latin"]
        except Exception as e:
            log(f"      ⚠️  Embedded Font කියවන්න බැරි වුනා ({fn}): {e}")
    return fams


def patch_all_sinhala_fonts():
    n = 0
    files = set()
    for line in sh("fc-list :lang=si file").stdout.splitlines():
        files.add(line.split(":")[0].strip())
    if os.path.isdir(FONT_DIR):
        files |= {os.path.join(FONT_DIR, x) for x in os.listdir(FONT_DIR)
                  if x.lower().endswith((".ttf", ".otf", ".ttc"))}
    for f in files:
        if f and os.path.isfile(f) and patch_font(f):
            n += 1
    sh("fc-cache -f")
    return n


def install_everything():
    log("=" * 60)
    log("🚀  Sinhala ASS Karaoke Burner Bot  —  Setup පටන් ගන්නවා")
    log("=" * 60)

    log("\n[1/4] 🖥️  GPU පරීක්ෂා කරනවා ...")
    g = sh("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader")
    if g.returncode == 0 and g.stdout.strip():
        log(f"      ✅ GPU හමුවුනා: {g.stdout.strip()}")
    else:
        log("      ⚠️  GPU හමු වුනේ නෑ! Runtime -> Change runtime type -> T4 GPU දාන්න.")
        log("          (දැනට CPU වලින් Encode කරනවා — ටිකක් ලෙමෙන්)")

    log("\n[2/4] 📦 FFmpeg + සිංහල Fonts Install වෙනවා (විනාඩියක් විතර යනවා) ...")
    need_apt = (not shutil.which("ffmpeg")) or (not shutil.which("fc-list")) or \
               ("sinhala" not in sh("fc-list").stdout.lower())
    if need_apt:
        sh("apt-get -qq update")
        r = sh("DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ffmpeg fontconfig "
               "fonts-noto-core fonts-lklug-sinhala", quiet=False)
        if r.returncode != 0:
            log("      ❌ apt install අසාර්ථකයි.")
            raise SystemExit(1)
    log("      ✅ FFmpeg + Sinhala Fonts සූදානම්")

    log("\n[3/4] 🐍 Python Libraries Install වෙනවා ...")
    sh(f"{sys.executable} -m pip uninstall -y pyrogram")
    for pkg in ("pyrofork", "tgcrypto", "nest_asyncio", "fonttools"):
        log(f"      ⏳ {pkg} ...")
        r = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-U", pkg],
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if r.returncode != 0:
            log(f"      ⚠️  {pkg} Install වුනේ නෑ:\n{r.stdout[-500:]}")
            if pkg == "pyrofork":
                raise SystemExit(1)
        else:
            log(f"      ✅ {pkg} OK")

    log("\n[4/4] 🎞️  NVENC (GPU Encoder) Test කරනවා ...")
    os.makedirs(FONT_DIR, exist_ok=True)
    sh(f"cp -n $(fc-list | grep -i sinhala | cut -d: -f1) {FONT_DIR}/ 2>/dev/null")
    sh("fc-cache -f")
    cnt = patch_all_sinhala_fonts()
    log(f"      ✅ සිංහල Fonts {cnt} ක ◌ (Dotted circle) ඉවත් කරා — පිල්ලම් කැඩෙන එක නවතී")


install_everything()

import asyncio, json, re, html, codecs, math, threading, importlib.util
import nest_asyncio

nest_asyncio.apply()

def speedup_upload_patch(workers=8):
    """Pyrogram Upload එකේ Parallel Workers 4 → 8 කරනවා (Import කරන්න කලින්)."""
    try:
        spec = importlib.util.find_spec("pyrogram")
        f = os.path.join(list(spec.submodule_search_locations)[0], "methods", "advanced", "save_file.py")
        txt = open(f, encoding="utf-8").read()
        new = re.sub(r"workers_count = \d+ if is_big else 1", f"workers_count = {workers} if is_big else 1", txt)
        if new != txt:
            open(f, "w", encoding="utf-8").write(new)
        return "pyrogram" not in sys.modules
    except Exception as e:
        log(f"      ⚠️  Upload Speed Patch අසාර්ථකයි: {e}")
        return False


UPLOAD_PATCHED = speedup_upload_patch()
if not UPLOAD_PATCHED:
    log("      ℹ️  Upload Speed Patch එක සම්පූර්ණයෙන් ක්‍රියාත්මක වෙන්න Runtime -> Restart session කරලා ආයෙ Run කරන්න.")

import logging
logging.getLogger("pyrogram.session.session").setLevel(logging.ERROR)   # "Waiting for 1 seconds" Warnings
from pyrogram import Client, filters, idle, StopTransmission
from pyrogram.enums import ParseMode
from pyrogram.file_id import FileId
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ----------------------------------------------------------------------------
#  2) ENCODER DETECTION
# ----------------------------------------------------------------------------
# Output Size එක Original එකට ආසන්නව තියාගන්න Source Bitrate එක පාදක කරගන්නවා.
# ලොකුවෙන් පෙනෙනවා නම් මේක අඩු කරන්න (උදා 1.1), Quality මදි නම් වැඩි කරන්න (උදා 1.5).
QUALITY_BOOST = 1.3

# /mod Command එකෙන් User කෙනෙක් කෙනෙක්ට වෙනස් කරගන්න පුළුවන්:
#   "stream" -> MP4  + Telegram එකේම Direct Streaming (Video විදිහට) — Default
#   "doc"    -> MKV  + Document විදිහට. ඔක්කොම Audio Tracks, Soft Subtitles, Fonts, Chapters ආරක්ෂා වෙනවා
DEFAULT_MODE = "stream"
MODES = {}   # user_id -> "stream" | "doc"
MODE_FMT = {"stream": "mp4", "doc": "mkv"}


def _v(b):  # "1234567" -> bits/sec string
    return str(int(b))


def _hevc_full(v, m, b):
    return ["-c:v", "hevc_nvenc", "-preset", "p5", "-tune", "hq", "-rc", "vbr",
            "-b:v", _v(v), "-maxrate", _v(m), "-bufsize", _v(b),
            "-rc-lookahead", "32", "-spatial-aq", "1", "-temporal-aq", "1", "-tag:v", "hvc1"]


def _hevc_simple(v, m, b):
    return ["-c:v", "hevc_nvenc", "-preset", "medium", "-rc", "vbr",
            "-b:v", _v(v), "-maxrate", _v(m), "-bufsize", _v(b), "-tag:v", "hvc1"]


def _h264_nv(v, m, b):
    return ["-c:v", "h264_nvenc", "-preset", "medium", "-rc", "vbr",
            "-b:v", _v(v), "-maxrate", _v(m), "-bufsize", _v(b), "-profile:v", "high"]


def _x264(v, m, b):
    return ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-maxrate", _v(m), "-bufsize", _v(b)]


# (label, output codec, builder)
GPU_ENCODERS = [("hevc_nvenc (T4 GPU)", "hevc", _hevc_full),
                ("hevc_nvenc (T4 GPU)", "hevc", _hevc_simple),
                ("h264_nvenc (T4 GPU)", "h264", _h264_nv)]
CPU_ENCODER = ("libx264 (CPU)", "h264", _x264)


def detect_gpu_encoder():
    for enc in GPU_ENCODERS:
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
               "color=c=black:s=640x360:d=0.5", "-pix_fmt", "yuv420p",
               *enc[2](1_000_000, 1_600_000, 3_200_000), "-f", "null", "-"]
        if subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode == 0:
            return enc
    return None


GPU_ENC = detect_gpu_encoder()
if GPU_ENC:
    log(f"      ✅ NVENC (T4 GPU Encoder) වැඩ — {GPU_ENC[0]} — ඉතා වේගවත් Encoding!")
else:
    log("      ⚠️  NVENC වැඩ නෑ — libx264 (CPU) භාවිතා කරනවා.")

# ----------------------------------------------------------------------------
#  3) HELPERS
# ----------------------------------------------------------------------------
VIDEO_EXT = (".mkv", ".mp4", ".avi", ".mov", ".webm", ".ts", ".m4v", ".flv", ".wmv", ".mpg", ".mpeg")
FONT_EXT = (".ttf", ".otf", ".ttc")
SUB_EXT = (".ass", ".ssa", ".srt")

S = {}                    # user_id -> session state
GPU_LOCK = asyncio.Lock()  # එකවර Encode එකක් විතරයි


class Cancelled(Exception):
    pass


def hsize(n):
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.2f} {u}" if u != "B" else f"{int(n)} B"
        n /= 1024


def htime(sec):
    sec = max(0, int(sec))
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def bar(pct, width=14):
    pct = max(0.0, min(100.0, pct))
    full = int(round(pct / 100 * width))
    return "█" * full + "░" * (width - full)


def render(icon, title, pct, lines):
    return (f"{icon} <b>{title}</b>\n\n"
            f"<code>[{bar(pct)}] {pct:5.1f}%</code>\n\n" + "\n".join(lines))


async def safe_edit(msg, text):
    try:
        await msg.edit_text(text, parse_mode=ParseMode.HTML)
    except Exception:
        pass


def make_transfer_cb(msg, st, icon, title):
    t0 = time.time()
    last = [0.0]

    async def cb(current, total):
        if st.get("cancel"):
            raise StopTransmission
        now = time.time()
        if current < total and now - last[0] < 3:
            return
        last[0] = now
        el = max(now - t0, 0.001)
        speed = current / el
        eta = (total - current) / speed if speed > 0 else 0
        pct = current * 100 / total if total else 0
        await safe_edit(msg, render(icon, title, pct, [
            f"📦 <b>{hsize(current)}</b> / {hsize(total)}",
            f"🚀 වේගය: <b>{hsize(speed)}/s</b>",
            f"⏳ ඉතිරි වෙලාව: <b>{htime(eta)}</b>   ⏱ ගතවුනේ: {htime(el)}",
        ]))
    return cb


# ----------------------------------------------------------------------------
#  4) ASS PREPARATION  (encoding fix + missing font → Noto Sans Sinhala)
# ----------------------------------------------------------------------------
def installed_families():
    out = sh("fc-list : family").stdout
    fams = set()
    for line in out.splitlines():
        for f in line.split(","):
            f = f.strip().lower()
            if f:
                fams.add(f)
    return fams


# සිංහල "පිල්ලම්" (Dependent vowel signs, Virama, ZWJ ...) — Karaoke Tag එකකින් වෙන් වුනොත්
# libass වලට Shape කරන්න බැරි වෙලා තනි ◌ ලකුණක් විදිහට පෙන්නනවා. ඒවා කලින් Syllable එකටම ඈඳනවා.
SI_MARKS = set("ංඃ්") | {chr(c) for c in range(0x0dcf, 0x0de0)} | {"ෲ", "ෳ"}
ZWJ = "‍"
HAS_SINHALA = re.compile("[඀-෿]")


def merge_sinhala_marks(text):
    toks = re.split(r"(\{[^}]*\})", text)   # ඔත්තේ index = {override tags}
    last = -1
    for i, t in enumerate(toks):
        if i % 2 == 1 or not t:
            continue
        if last >= 0:
            prev, cur = toks[last], t
            while cur and (cur[0] in SI_MARKS or prev[-1] == ZWJ):
                prev += cur[0]
                cur = cur[1:]
            toks[last], toks[i] = prev, cur
        if toks[i]:
            last = i
    return "".join(toks)


def read_text_file(path):
    raw = open(path, "rb").read()
    if raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        return raw.decode("utf-16")
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def prep_ass(src, dst, emb=None):
    """emb: වීඩියෝව ඇතුළේ Embed කරපු Fonts ({family(lower): {"si", "latin"}}). ඒවා Install කරපු ඒවා වගේම ගන්නවා."""
    emb = emb or {}
    text = read_text_file(src)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")

    # ---- Pass 1: Events format + සිංහල අඩංගු Styles හොයනවා, Karaoke Marks ඈඳනවා ----
    sec, ev_cols, text_idx = "", [], 9
    si_styles, merged = set(), 0
    for n, line in enumerate(lines):
        low = line.strip().lower()
        if low.startswith("["):
            sec = low
        elif sec.startswith("[events") and low.startswith("format:"):
            ev_cols = [c.strip().lower() for c in line.split(":", 1)[1].split(",")]
            text_idx = len(ev_cols) - 1
        elif sec.startswith("[events") and low.startswith("dialogue:"):
            head, body = line.split(":", 1)
            parts = body.lstrip().split(",", text_idx)
            if len(parts) <= text_idx:
                continue
            if HAS_SINHALA.search(parts[text_idx]):
                st_idx = ev_cols.index("style") if "style" in ev_cols else 3
                si_styles.add(parts[st_idx].strip().lstrip("*").lower())
                new = merge_sinhala_marks(parts[text_idx])
                if new != parts[text_idx]:
                    merged += 1
                    parts[text_idx] = new
                    lines[n] = head + ": " + ",".join(parts)

    # ---- Pass 2: නැති Fonts ආදේශ කරනවා ----
    fams = installed_families()
    si_font = "Noto Sans Sinhala" if "noto sans sinhala" in fams else "Noto Sans"
    latin_font = "Noto Sans" if "noto sans" in fams else si_font
    replaced, spacing_fixed, used_emb = set(), set(), set()
    en_override = {}   # සිංහල Style එකක English-only Line වලට යෙදිය යුතු Embedded English Font

    def fix(name, sinhala):
        clean = name.strip().lstrip("@")
        if not clean:
            return name
        e = emb.get(clean.lower())
        if e and e["si" if sinhala else "latin"]:
            used_emb.add(clean)       # වීඩියෝවේ Font එකම පාවිච්චි කරනවා
            return name
        if clean.lower() in fams:
            return name
        replaced.add(clean)
        return si_font if sinhala else latin_font

    sec, font_idx, name_idx, sp_idx = "", 1, 0, -1
    for n, line in enumerate(lines):
        low = line.strip().lower()
        if low.startswith("["):
            sec = low
        elif sec.startswith("[v4") and low.startswith("format:"):
            cols = [c.strip().lower() for c in line.split(":", 1)[1].split(",")]
            font_idx = cols.index("fontname") if "fontname" in cols else 1
            name_idx = cols.index("name") if "name" in cols else 0
            sp_idx = cols.index("spacing") if "spacing" in cols else -1
        elif sec.startswith("[v4") and low.startswith("style:"):
            head, body = line.split(":", 1)
            parts = body.lstrip().split(",")
            if len(parts) > font_idx:
                is_si = parts[name_idx].strip().lstrip("*").lower() in si_styles
                orig_font = parts[font_idx]
                parts[font_idx] = fix(orig_font, is_si)
                e = emb.get(orig_font.strip().lstrip("@").lower())
                if is_si and parts[font_idx] != orig_font and e and e["latin"]:
                    # Font එකේ සිංහල නෑ -> Style එකට සිංහල Font. නමුත් English Lines වලට වීඩියෝවේ Font එකම.
                    en_override[parts[name_idx].strip().lstrip("*").lower()] = orig_font.strip().lstrip("@")
                # Letter-spacing ≠ 0 නම් libass සිංහල Shaping කැඩෙනවා (ෙ ො ේ වැරදි තැනට යනවා)
                if is_si and 0 <= sp_idx < len(parts):
                    try:
                        if float(parts[sp_idx]) != 0:
                            spacing_fixed.add(parts[name_idx].strip())
                    except ValueError:
                        pass
                    parts[sp_idx] = "0"
                lines[n] = head + ": " + ",".join(parts)
        elif sec.startswith("[events") and low.startswith(("dialogue:", "comment:")):
            sinhala = bool(HAS_SINHALA.search(line))
            if en_override and not sinhala and low.startswith("dialogue:") and ev_cols:
                head, body = line.split(":", 1)
                parts = body.lstrip().split(",", text_idx)
                st_idx = ev_cols.index("style") if "style" in ev_cols else 3
                font = en_override.get(parts[st_idx].strip().lstrip("*").lower()) if len(parts) > text_idx else None
                if font:
                    parts[text_idx] = "{\\fn" + font + "}" + parts[text_idx]
                    line = head + ": " + ",".join(parts)
                    used_emb.add(font)
            if "\\fn" in line:
                line = re.sub(r"\\fn([^\\}]+)", lambda m: "\\fn" + fix(m.group(1), sinhala), line)
            if sinhala and "\\fsp" in line:
                line = re.sub(r"\\fsp[-+]?[\d.]*", r"\\fsp0", line)
            lines[n] = line

    with open(dst, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines))
    return sorted(replaced), merged, sorted(used_emb)


# ----------------------------------------------------------------------------
#  5) FFMPEG
# ----------------------------------------------------------------------------
async def probe(path):
    p = await asyncio.create_subprocess_exec(
        "ffprobe", "-v", "error", "-print_format", "json", "-show_format", "-show_streams", path,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    out, _ = await p.communicate()
    j = json.loads(out or b"{}")
    if not any(x.get("codec_type") == "video" for x in j.get("streams", [])):
        raise RuntimeError("වීඩියෝ ගොනුව කියවන්න බැරි වුනා (Download එක අසම්පූර්ණ/කැඩිලා වෙන්න පුළුවන්). ආයෙ එවන්න.")
    v = next(s for s in j["streams"] if s["codec_type"] == "video")
    dur = float(j["format"].get("duration") or v.get("duration") or 0)

    def sbps(s):
        for val in (s.get("bit_rate"), (s.get("tags") or {}).get("BPS")):
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
        return 0.0

    audios = [s for s in j["streams"] if s["codec_type"] == "audio"]
    a_bps = sum(sbps(a) or 128_000 for a in audios)
    total = (os.path.getsize(path) * 8 / dur) if dur else float(j["format"].get("bit_rate") or 0)
    a_bps = min(a_bps, total * 0.5)
    real_v = max(total - a_bps, total * 0.3)       # ගොනුවේ සැබෑ Size එකෙන් ගණන් බැලූ Video Bitrate
    v_bps = min(sbps(v) or real_v, real_v * 1.15)  # Tags වල බොරු ලොකු අගයන් පිළිගන්නෙ නෑ
    v_bps = v_bps or 1_500_000
    return {"dur": dur, "w": int(v["width"]), "h": int(v["height"]),
            "vcodec": v.get("codec_name", ""), "v_bps": v_bps,
            "acodecs": [a.get("codec_name", "") for a in audios]}


def target_bitrate(info, out_codec):
    """Source Bitrate එකට ආසන්න (Size එක Original එක තරම් තියාගන්න) Bitrate එකක්."""
    efficient = info["vcodec"] in ("hevc", "av1", "vp9")
    if out_codec == "hevc":
        f = QUALITY_BOOST if efficient else 0.9
    else:
        f = QUALITY_BOOST * 1.5 if efficient else 1.3
    return min(max(info["v_bps"] * f, 350_000), 30_000_000)


async def encode(st, status, video, workdir, duration, venc, aenc, label):
    vf = "ass=sub.ass:fontsdir=vfonts:shaping=complex,format=yuv420p"   # vfonts = වීඩියෝවේ Fonts + User Fonts
    if st["fmt"] == "mkv":
        venc = [x for i, x in enumerate(venc) if x != "-tag:v" and not (i and venc[i - 1] == "-tag:v")]
        maps = ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?",
                "-map_chapters", "0", "-map_metadata", "0", "-c:s", "copy", "-c:t", "copy"]
        tail = []
    else:
        maps = ["-map", "0:v:0", "-map", "0:a?", "-sn", "-dn", "-map_chapters", "-1"]
        tail = ["-movflags", "+faststart"]
    cmd = ["ffmpeg", "-hide_banner", "-y", "-i", video, *maps,
           "-vf", vf, *venc, *aenc, *tail,
           "-progress", "pipe:1", "-nostats", "output." + st["fmt"]]
    logf = open(os.path.join(workdir, "ffmpeg.log"), "wb")
    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=workdir, stdout=asyncio.subprocess.PIPE, stderr=logf)
    st["proc"] = proc
    t0, last = time.time(), 0.0
    cur = {"t": 0.0, "fps": "0", "speed": "0x"}
    while True:
        line = await proc.stdout.readline()
        if not line:
            break
        k, _, v = line.decode(errors="ignore").strip().partition("=")
        if k in ("out_time_us", "out_time_ms"):
            try:
                cur["t"] = int(v) / 1_000_000
            except ValueError:
                pass
        elif k == "fps":
            cur["fps"] = v
        elif k == "speed":
            cur["speed"] = v
        elif k == "progress":
            now = time.time()
            if v == "continue" and now - last < 3:
                continue
            last = now
            pct = cur["t"] * 100 / duration if duration else 0
            try:
                sp = float(cur["speed"].replace("x", "").strip() or 0)
            except ValueError:
                sp = 0
            eta = (duration - cur["t"]) / sp if sp > 0 else 0
            await safe_edit(status, render("🔥", "Subtitle Burn වෙමින් පවතී", pct, [
                f"🎞 වීඩියෝ කාලය: <b>{htime(cur['t'])}</b> / {htime(duration)}",
                f"⚡ වේගය: <b>{cur['speed']}</b>   🖼 FPS: {cur['fps']}",
                f"⏳ ඉතිරි වෙලාව: <b>{htime(eta)}</b>   ⏱ ගතවුනේ: {htime(now - t0)}",
                f"🧠 Encoder: <code>{label}</code>",
            ]))
    rc = await proc.wait()
    logf.close()
    st["proc"] = None
    if st.get("cancel"):
        raise Cancelled
    return rc == 0


async def make_thumb(video, workdir, duration):
    thumb = os.path.join(workdir, "thumb.jpg")
    p = await asyncio.create_subprocess_exec(
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", str(min(5, duration / 2)),
        "-i", video, "-frames:v", "1", "-vf", "scale=320:-1", thumb,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await p.wait()
    return thumb if os.path.exists(thumb) else None


# ----------------------------------------------------------------------------
#  5b) FAST PARALLEL DOWNLOAD  (වීඩියෝව කොටස් 8 කට කඩලා එකවර Download කරනවා)
# ----------------------------------------------------------------------------
DL_PARTS = 8
CHUNK = 1024 * 1024


async def fast_download(client, message, path, progress):
    media = message.video or message.document or message.animation
    size = media.file_size or 0
    if size < 8 * CHUNK:
        return await message.download(file_name=path, progress=progress)
    fid = FileId.decode(media.file_id)
    n_chunks = math.ceil(size / CHUNK)
    parts = min(DL_PARTS, n_chunks)
    per = math.ceil(n_chunks / parts)
    lock = threading.Lock()
    done = [0]
    with open(path, "wb") as f:
        f.truncate(size)
    fh = open(path, "r+b")

    def write(off, data):
        with lock:
            fh.seek(off)
            fh.write(data)

    start_lock = asyncio.Lock()   # Auth Export/Import එකිනෙකට පැටලෙන්න දෙන්නෙ නෑ (AUTH_BYTES_INVALID)

    async def seg(i):
        start = i * per
        cnt = min(per, n_chunks - start)
        if cnt <= 0:
            return
        off = begin = start * CHUNK
        agen = client.get_file(fid, size, limit=cnt, offset=start).__aiter__()

        async def put(chunk):
            nonlocal off
            await asyncio.to_thread(write, off, chunk)
            off += len(chunk)
            done[0] += len(chunk)
            await progress(min(done[0], size), size)

        async with start_lock:           # Session එක පිහිටුවා පළමු කොටස එනකම් එකින් එක
            try:
                first = await agen.__anext__()
            except StopAsyncIteration:
                first = None
        if first is not None:
            await put(first)
            async for chunk in agen:
                await put(chunk)
        # get_file Error නිහඬව නවත්තනවා — අඩුවෙන් ආවොත් Fail කරනවා (නැත්නම් Zero-filled ගොනුවක් හැදෙනවා)
        if off - begin < min(cnt * CHUNK, size - begin):
            raise RuntimeError(f"කොටස {i} සම්පූර්ණ වුනේ නෑ")

    tasks = [asyncio.ensure_future(seg(i)) for i in range(parts)]
    try:
        await asyncio.gather(*tasks)
    except BaseException:
        for t in tasks:
            t.cancel()
        fh.close()
        raise
    fh.close()
    return path if os.path.getsize(path) == size else None


# ----------------------------------------------------------------------------
#  5c) SRT -> ASS
# ----------------------------------------------------------------------------
def srt_to_ass(src, dst, w, h):
    text = read_text_file(src).replace("\r\n", "\n").replace("\r", "\n")
    tc = re.compile(r"(\d+):(\d+):(\d+)[,.](\d+)\s*-->\s*(\d+):(\d+):(\d+)[,.](\d+)")

    def ts(hh, mm, ss, ms):
        cs = int(round(int(ms.ljust(3, "0")[:3]) / 10))
        total = int(hh) * 3600 + int(mm) * 60 + int(ss) + cs // 100
        return f"{total // 3600}:{total % 3600 // 60:02d}:{total % 60:02d}.{cs % 100:02d}"

    def color(m):
        c = m.group(1).lstrip("#")
        return "{\\c&H%s%s%s&}" % (c[4:6], c[2:4], c[0:2])

    def conv(t):
        an = re.search(r"\{\\an[1-9]\}", t)
        pos = an.group(0) if an else ""
        t = re.sub(r"\{[^}]*\}", "", t)
        t = t.replace("{", "(").replace("}", ")")
        t = re.sub(r"<(/?)(i|b|u|s)>", lambda m: "{\\%s%d}" % (m.group(2).lower(), 0 if m.group(1) else 1),
                   t, flags=re.I)
        t = re.sub(r"<font[^>]*color=[\"']?(#?[0-9a-fA-F]{6})[\"']?[^>]*>", color, t, flags=re.I)
        t = re.sub(r"</font>", r"{\\c}", t, flags=re.I)
        t = re.sub(r"<[^>]+>", "", t)
        return pos + t.strip().replace("\n", "\\N")

    events = []
    for block in re.split(r"\n\s*\n", text.strip()):
        lines = block.split("\n")
        for i, l in enumerate(lines):
            m = tc.search(l)
            if m:
                body = conv("\n".join(lines[i + 1:]))
                if body:
                    g = m.groups()
                    events.append(f"Dialogue: 0,{ts(*g[:4])},{ts(*g[4:])},Default,,0,0,0,,{body}")
                break

    fams = installed_families()
    font = "Noto Sans Sinhala" if "noto sans sinhala" in fams else "Noto Sans"
    size = max(18, round(h * 0.058))
    ass = (f"[Script Info]\nScriptType: v4.00+\nPlayResX: {w}\nPlayResY: {h}\nWrapStyle: 0\n"
           "ScaledBorderAndShadow: yes\n\n[V4+ Styles]\n"
           "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, "
           "Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
           "Alignment, MarginL, MarginR, MarginV, Encoding\n"
           f"Style: Default,{font},{size},&H00FFFFFF,&H000000FF,&H00000000,&H80000000,-1,0,0,0,100,100,0,0,1,"
           f"{max(1.5, h / 360):.1f},{max(0.5, h / 720):.1f},2,{round(w * 0.04)},{round(w * 0.04)},{round(h * 0.05)},1\n\n"
           "[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"
           + "\n".join(events) + "\n")
    with open(dst, "w", encoding="utf-8", newline="\n") as f:
        f.write(ass)
    return len(events)


# ----------------------------------------------------------------------------
#  6) BOT
# ----------------------------------------------------------------------------
app = Client("ass_burner_bot", api_id=int(API_ID), api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, parse_mode=ParseMode.HTML, sleep_threshold=60,
             max_concurrent_transmissions=16)

HELP = (
    "👋 <b>Sinhala ASS Karaoke Burner Bot</b>\n\n"
    "1️⃣ මට <b>වීඩියෝ එක</b> එවන්න\n"
    "2️⃣ ඊට පස්සෙ <b>.ass / .srt Subtitle ගොනුව</b> එවන්න\n"
    "3️⃣ මම T4 GPU එකෙන් Subtitle එක වීඩියෝවට Burn කරලා ආපහු එවනවා 🚀\n\n"
    "🔤 විශේෂ Font එකක් ඕන නම් <b>.ttf / .otf</b> ගොනුව ඒ විදිහටම එවන්න.\n"
    "⚙️ Upload Mode (Streaming / Document) තෝරන්න: /mod\n"
    "❌ නවත්තන්න: /cancel"
)


def new_state(uid, chat):
    d = os.path.join(WORK_ROOT, str(uid))
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d, exist_ok=True)
    return {"dir": d, "chat": chat, "video": None, "ass": None, "vname": "video",
            "busy": False, "dl": False, "cancel": False, "proc": None}


def get_state(uid, chat):
    if uid not in S:
        S[uid] = new_state(uid, chat)
    S[uid]["chat"] = chat
    return S[uid]


def try_start(uid):
    st = S.get(uid)
    if st and st["video"] and st["ass"] and not st["busy"] and not st["dl"]:
        st["busy"] = True
        asyncio.get_event_loop().create_task(process(uid))


@app.on_message(filters.command(["start", "help"]) & filters.private)
async def cmd_start(c, m):
    await m.reply_text(HELP)


@app.on_message(filters.command("cancel") & filters.private)
async def cmd_cancel(c, m):
    st = S.get(m.from_user.id)
    if not st:
        return await m.reply_text("ℹ️ Cancel කරන්න දෙයක් නෑ.")
    st["cancel"] = True
    p = st.get("proc")
    if p and p.returncode is None:
        try:
            p.kill()
        except Exception:
            pass
    if not st["busy"] and not st["dl"]:
        shutil.rmtree(st["dir"], ignore_errors=True)
        S.pop(m.from_user.id, None)
    await m.reply_text("🛑 Cancel කරා.")


def mod_view(uid):
    cur = MODES.get(uid, DEFAULT_MODE)
    text = (
        "⚙️ <b>Upload Mode තෝරන්න</b>\n\n"
        "🎬 <b>Streaming</b> — MP4. Telegram එකේම කෙලින් Play/Seek වෙනවා. "
        "සියලුම Audio Tracks ඉතුරු වෙනවා, නමුත් Soft Subtitles/Fonts ඉතුරු වෙන්නෙ නෑ.\n\n"
        "📁 <b>Document</b> — MKV ගොනුවක් විදිහට. Audio Tracks, Soft Subtitles, Fonts, Chapters "
        "ඔක්කොම ආරක්ෂා වෙනවා. Download කරලා Play කරන්න ඕන.\n\n"
        f"දැනට තෝරලා තියෙන්නෙ: <b>{'🎬 Streaming' if cur == 'stream' else '📁 Document'}</b>")
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton(("✅ " if cur == "stream" else "") + "🎬 Streaming", callback_data="mod_stream"),
        InlineKeyboardButton(("✅ " if cur == "doc" else "") + "📁 Document", callback_data="mod_doc"),
    ]])
    return text, kb


@app.on_message(filters.command("mod") & filters.private)
async def cmd_mod(c, m):
    text, kb = mod_view(m.from_user.id)
    await m.reply_text(text, reply_markup=kb)


@app.on_callback_query(filters.regex(r"^mod_(stream|doc)$"))
async def cb_mod(c, q):
    new = q.data.split("_", 1)[1]
    if MODES.get(q.from_user.id, DEFAULT_MODE) == new:
        return await q.answer("දැනටමත් මේ Mode එකයි ✅")
    MODES[q.from_user.id] = new
    text, kb = mod_view(q.from_user.id)
    try:
        await q.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass
    await q.answer("🎬 Streaming Mode" if new == "stream" else "📁 Document Mode")


@app.on_message(filters.command("gpu") & filters.private)
async def cmd_gpu(c, m):
    r = sh("nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total --format=csv,noheader")
    enc = GPU_ENC[0] if GPU_ENC else "libx264 (CPU)"
    await m.reply_text(f"🖥 <code>{html.escape(r.stdout.strip() or 'GPU නෑ')}</code>\n🎞 Encoder: <b>{enc}</b>")


@app.on_message(filters.private & (filters.video | filters.document | filters.animation))
async def on_file(c, m):
    uid = m.from_user.id
    doc = m.document
    name = (doc.file_name if doc and doc.file_name else "").lower()
    ext = os.path.splitext(name)[1]

    if doc and ext in SUB_EXT:
        kind = "sub"
    elif doc and ext in FONT_EXT:
        kind = "font"
    elif m.video or m.animation or (doc and (ext in VIDEO_EXT or (doc.mime_type or "").startswith("video/"))):
        kind = "video"
    else:
        return await m.reply_text("⚠️ වීඩියෝ එකක්, .ass/.srt ගොනුවක් හෝ Font (.ttf/.otf) එකක් විතරක් එවන්න.")

    st = get_state(uid, m.chat.id)
    if st["busy"]:
        return await m.reply_text("⏳ දැනට වැඩක් කරමින් ඉන්නවා. /cancel කරන්න හෝ ඉවර වෙනකම් ඉන්න.")

    # ---------- FONT ----------
    if kind == "font":
        os.makedirs(FONT_DIR, exist_ok=True)
        fpath = os.path.join(FONT_DIR, os.path.basename(doc.file_name))
        await m.download(file_name=fpath)
        await asyncio.to_thread(patch_font, fpath)
        await asyncio.to_thread(sh, "fc-cache -f")
        return await m.reply_text(f"🔤 Font එක Install කරා: <code>{html.escape(doc.file_name)}</code>")

    # ---------- SUBTITLE ----------
    if kind == "sub":
        path = os.path.join(st["dir"], "input" + ext)
        await m.download(file_name=path)
        st["ass"] = path
        if st["video"]:
            await m.reply_text("✅ Subtitle ලැබුනා! Burn කිරීම පටන් ගන්නවා... 🔥")
            return try_start(uid)
        msg = "✅ Subtitle ලැබුනා!"
        if st["dl"]:
            msg += " වීඩියෝ එක Download වෙලා ඉවර වුනාම ස්වයංක්‍රීයව පටන් ගන්නවා."
        else:
            msg += " දැන් <b>වීඩියෝ එක</b> එවන්න 🎬"
        return await m.reply_text(msg)

    # ---------- VIDEO ----------
    if st["dl"]:
        return await m.reply_text("⏳ වීඩියෝ එකක් Download වෙමින් පවතී.")
    fname = (doc.file_name if doc and doc.file_name else
             (m.video.file_name if m.video and m.video.file_name else "video.mp4"))
    st["vname"] = os.path.splitext(os.path.basename(fname))[0] or "video"
    st["cancel"], st["dl"] = False, True
    status = await m.reply_text("📥 Download පටන් ගන්නවා...")
    try:
        vpath = os.path.join(st["dir"], "input" + (os.path.splitext(fname)[1] or ".mp4"))
        cb = make_transfer_cb(status, st, "📥", "වීඩියෝ එක Download වෙමින් පවතී")
        try:
            res = await fast_download(app, m, vpath, cb)
        except StopTransmission:
            res = None
        except Exception as e:
            if st["cancel"]:
                raise Cancelled
            log(f"Fast download අසාර්ථකයි ({e}); සාමාන්‍ය Download එකට මාරු වෙනවා")
            res = await m.download(file_name=vpath, progress=cb)
        if not res or st["cancel"]:
            raise Cancelled
        st["video"] = res
        size = os.path.getsize(res)
        await safe_edit(status, f"✅ <b>Download සම්පූර්ණයි</b>  ({hsize(size)})")
    except Cancelled:
        S.pop(uid, None)
        shutil.rmtree(os.path.join(WORK_ROOT, str(uid)), ignore_errors=True)
        return await safe_edit(status, "🛑 Download එක Cancel කරා.")
    except Exception as e:
        S.pop(uid, None)
        return await safe_edit(status, f"❌ Download අසාර්ථකයි:\n<code>{html.escape(str(e))}</code>")
    finally:
        if uid in S:
            S[uid]["dl"] = False

    if st["ass"]:
        await m.reply_text("✅ Subtitle කලින් ලැබිලා තියෙනවා. Burn කිරීම පටන් ගන්නවා... 🔥")
        try_start(uid)
    else:
        await m.reply_text("📄 දැන් <b>.ass / .srt Subtitle ගොනුව</b> එවන්න.")


async def process(uid):
    st = S[uid]
    chat, d = st["chat"], st["dir"]
    st["fmt"] = MODE_FMT[MODES.get(uid, DEFAULT_MODE)]
    status = await app.send_message(chat, "⏳ GPU Queue එකේ ඉන්නවා...")
    try:
        async with GPU_LOCK:
            t_all = time.time()
            await safe_edit(status, "🧩 ASS ගොනුව සකස් කරනවා (Encoding / Fonts)...")
            info = await probe(st["video"])
            duration, w, h = info["dur"], info["w"], info["h"]
            sub_src = st["ass"]
            if sub_src.lower().endswith(".srt"):
                sub_src = os.path.join(d, "converted.ass")
                n_ev = await asyncio.to_thread(srt_to_ass, st["ass"], sub_src, w, h)
                if n_ev == 0:
                    raise RuntimeError("SRT ගොනුවෙන් Subtitle Lines කිසිවක් කියවන්න බැරි වුනා.")
            vfonts = os.path.join(d, "vfonts")
            emb = await asyncio.to_thread(extract_embedded_fonts, st["video"], vfonts)
            if os.path.isdir(FONT_DIR):
                for fn in os.listdir(FONT_DIR):
                    if fn.lower().endswith(FONT_EXT) and not os.path.exists(os.path.join(vfonts, fn)):
                        shutil.copy2(os.path.join(FONT_DIR, fn), vfonts)
            replaced, merged, used_emb = await asyncio.to_thread(
                prep_ass, sub_src, os.path.join(d, "sub.ass"), emb)

            copy_ok = all(a in ("aac", "mp3", "ac3", "eac3", "opus") for a in info["acodecs"])
            if st["fmt"] == "mkv":
                copy_ok = True   # MKV වලට ඕනෑම Audio Codec එකක් Copy කරන්න පුළුවන්
            a_opts = ([["-c:a", "copy"]] if copy_ok else []) + [["-c:a", "aac", "-b:a", "160k"]]
            encoders = (GPU_ENCODERS[GPU_ENCODERS.index(GPU_ENC):] if GPU_ENC else []) + [CPU_ENCODER]
            attempts = []
            for label, codec, build in encoders:
                tb = target_bitrate(info, codec)
                venc = build(tb, tb * 1.6, tb * 3.2)
                for aenc in a_opts:
                    attempts.append((venc, aenc, f"{label} @ {tb / 1000:.0f} kbps"))

            ok, used, t_enc = False, "", time.time()
            for venc, aenc, label in attempts:
                t_enc = time.time()
                if os.path.exists(os.path.join(d, "output." + st["fmt"])):
                    os.remove(os.path.join(d, "output." + st["fmt"]))
                ok = await encode(st, status, st["video"], d, duration, venc, aenc, label)
                if ok:
                    used = label
                    break
            if not ok:
                tail = open(os.path.join(d, "ffmpeg.log"), "rb").read()[-900:].decode(errors="ignore")
                raise RuntimeError("FFmpeg අසාර්ථකයි:\n" + tail)
            enc_time = time.time() - t_enc

            final = os.path.join(d, re.sub(r'[\\/:*?"<>|]', "_", st["vname"]) + "_Sinhala." + st["fmt"])
            os.replace(os.path.join(d, "output." + st["fmt"]), final)
            thumb = await make_thumb(final, d, duration)
            fsize = os.path.getsize(final)
            if fsize > 2000 * 1024 * 1024:
                raise RuntimeError(f"ගොනුව {hsize(fsize)} — Telegram Bot සීමාව (2GB) ඉක්මවනවා.")

            await safe_edit(status, render("📤", "Upload පටන් ගන්නවා", 0, [f"📦 {hsize(fsize)}"]))
            cap = (f"🎬 <b>{html.escape(st['vname'])}</b>\n"
                   f"✨ Sinhala Karaoke Subtitles Burned\n"
                   f"⚡ {used} • ⏱ {htime(enc_time)}")
            up_cb = make_transfer_cb(status, st, "📤", "Upload වෙමින් පවතී")
            if st["fmt"] == "mkv":
                await app.send_document(chat, final, caption=cap, thumb=thumb, force_document=True,
                                        progress=up_cb)
            else:
                await app.send_video(
                    chat, final, caption=cap, duration=int(duration), width=w, height=h,
                    thumb=thumb, supports_streaming=True, progress=up_cb)
            if st.get("cancel"):
                raise Cancelled
            await status.delete()
            in_size = os.path.getsize(st["video"])
            note = (f"\n📉 Size: {hsize(in_size)} → <b>{hsize(fsize)}</b>"
                    f"\n🔤 සිංහල පිල්ලම් Fix කරපු Karaoke Lines: {merged}")
            if used_emb:
                note += f"\n🔠 වීඩියෝව ඇතුළේ Fonts පාවිච්චි කරා: {html.escape(', '.join(used_emb[:6]))}"
            if replaced:
                shown = ", ".join(replaced[:6]) + (f" +{len(replaced) - 6} තවත්" if len(replaced) > 6 else "")
                note += (f"\n\nℹ️ <i>Colab එකේ නැති Fonts ({html.escape(shown)}) Noto Sans වලට ආදේශ කරා. "
                         "මේක Error එකක් නෙවෙයි. ඒ Fonts .ttf විදිහට එව්වොත් ඒවාම පාවිච්චි වෙනවා.</i>")
            await app.send_message(
                chat, f"✅ <b>සම්පූර්ණයි!</b>\n⏱ මුළු කාලය: {htime(time.time() - t_all)}{note}")
    except Cancelled:
        await safe_edit(status, "🛑 Cancel කරා.")
    except Exception as e:
        await safe_edit(status, f"❌ <b>දෝෂයක්:</b>\n<code>{html.escape(str(e)[-900:])}</code>")
    finally:
        shutil.rmtree(d, ignore_errors=True)
        S.pop(uid, None)


async def main():
    await app.start()
    me = await app.get_me()
    log("\n" + "=" * 60)
    log(f"✅  Bot Online!  →  @{me.username}")
    log("    Telegram එකේ /start එවලා පටන් ගන්න. නවත්තන්න Cell එක Stop කරන්න.")
    log("=" * 60)
    await idle()
    await app.stop()


os.makedirs(WORK_ROOT, exist_ok=True)
try:
    asyncio.get_event_loop().run_until_complete(main())
except KeyboardInterrupt:
    log("🛑 Bot නැවතුවා.")
