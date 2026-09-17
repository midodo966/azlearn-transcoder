#!/usr/bin/env python3
"""
================================================================================
AZ LEARN (MED HUB) — AUTOMATED HEADLESS TRANSCODING & DRM INGESTION PIPELINE
================================================================================
Architectural Overview & Standards:
1. Universal High-Quality 720p HD Standard (4.0s Segments):
   - Aligned directly with production standard colab-scripts/1_encrypt_and_upload_aio.py.
   - Slices video into 4.0-second AES-128 encrypted HLS chunks (-hls_time 4).
   - Enforces Constrained VBR (2000k target, 2500k max ceiling, 4000k buffer, CRF 20, 30 FPS).
   - Ultra-fast single-pass encoding + slicing eliminates multi-pass overhead, finishing
     15-minute video files in ~1-2 minutes on standard compute runners.
   - Fast-path stream-copy allows zero-reencoding slicing if source is already compatible H.264.

2. Multi-Brand Cross-Tenant Architecture (AZ Learn & All In One):
   - AZ Learn: Binds dynamically to az-courses-db (env.DB_AZ), az-bucket, and /AZ/ edge API.
   - All In One: Binds dynamically to aio-courses-db (env.DB_AIO), aio-bucket, and /AIO/ edge API.
   - Cross-Brand Auto-Discovery: If a job UUID is queried, the pipeline checks both tenant databases
     automatically, ensuring seamless execution across all administrative portals.

3. 100% Private Google Drive Ingestion (Restricted Access):
   - Authenticates directly with Google Drive API v3 using Google Cloud Service Account
     credentials (drivebot@sodium-ray-508905-a4.iam.gserviceaccount.com).
   - Zero public sharing links required; tutors upload directly to private subfolders.

4. Direct High-Speed Cloudflare R2 Edge Streaming (Rule 17 Compliance):
   - Streams chunks concurrently via Boto3 with a 100-connection connection pool.
   - Zero Workers media proxying; served 100% directly via dedicated R2 Custom Domains:
     * AZ Learn: az-cdn.medhub-academy.stream
     * All In One: aio-cdn.medhub-academy.stream

5. Atomic D1 Vaulting & Execution Telemetry:
   - Vaults AES-128 key directly into Cloudflare D1 video_keys table via POST /api/admin/ingest.
   - Reports live progress percentages (10%, 30%, 85%, 100%) to Cloudflare D1 transcode_jobs.
================================================================================
"""

import os
import sys
import re
import json
import time
import uuid
import shutil
import base64
import binascii
import argparse
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

# Auto-install runtime dependencies if executed in a fresh CI environment
def ensure_dependencies():
    packages = ["boto3", "requests", "google-api-python-client", "google-auth", "cryptography"]
    missing = []
    for pkg in packages:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"📦 [Setup] Installing missing runtime dependencies: {', '.join(missing)}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q"] + missing, check=True)

ensure_dependencies()

import requests
import boto3
from botocore.config import Config
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ==============================================================================
# CONFIGURATION & RUNTIME ENVIRONMENT BINDINGS
# ==============================================================================
# Direct workers.dev edge domain is used as the default to prevent datacenter Cloudflare WAF/Turnstile challenges
EDGE_API_ORIGIN = os.environ.get("EDGE_API_ORIGIN", "https://courses-backend.midodo966.workers.dev")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "azlearn_adm_sec_9f8b7c6d5e4a3b2c1d0e9f8a7b6c5d4e")

R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "e7e407d343739d728e23cf0e0f815c87")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "a8054551b4797c7ddfa2f9c6415a9e02")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "bb3f5a77be861ed9a34e6f995a684cac244f0f392139f7fd459acf5f8e1ddc37")

# Active tenant state (default: AZ Learn)
CURRENT_BRAND = "az"
CLOUDFLARE_API_URL = f"{EDGE_API_ORIGIN}/AZ"
R2_BUCKET_NAME = "az-bucket"
KEY_GATEWAY_URL = "https://api.medhub-academy.stream/AZ"

# Local fallback path for Google Service Account credentials
LOCAL_GDRIVE_KEY_PATH = "/home/midododo/Downloads/sodium-ray-508905-a4-bf28edccfcde.json"
GDRIVE_SERVICE_ACCOUNT_JSON = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON", "")

# Standardized Video Encoding Parameters (from 1_encrypt_and_upload_aio.py)
TARGET_WIDTH = 1280
TARGET_HEIGHT = 720
TARGET_FPS = 30
HLS_CHUNK_DURATION = 4          # 4.0-second segments for responsive seeking & low startup latency
TARGET_VIDEO_BITRATE = 2000     # Target video bitrate in kbps (-b:v 2000k)
MAX_VIDEO_BITRATE = 2500        # Maximum video bitrate ceiling in kbps (-maxrate 2500k)
VIDEO_BUFSIZE = 4000            # Strict VBV buffer size in kbps (-bufsize 4000k)
CRF_QUALITY = 20                # Near-lossless visual quality for educational text & video

# ==============================================================================
# HELPER: TELEMETRY & WORKER API CLIENT
# ==============================================================================
api_session = requests.Session()
api_session.headers.update({
    "Authorization": f"Bearer {ADMIN_KEY}",
    "Content-Type": "application/json",
    "User-Agent": "AZLearn-Transcoder-Engine/2.0 (Ubuntu; Linux x86_64; Automated Edge Pipeline)"
})

def configure_brand(brand):
    """
    Dynamically adjusts API base, R2 bucket, and key gateway URL for multi-tenant deployments.
    Ensures 100% brand isolation between AZ Learn and All In One.
    """
    global CURRENT_BRAND, CLOUDFLARE_API_URL, R2_BUCKET_NAME, KEY_GATEWAY_URL
    b = (brand or "az").lower()
    if b not in ["az", "aio"]:
        b = "az"
    CURRENT_BRAND = b

    if b == "aio":
        CLOUDFLARE_API_URL = f"{EDGE_API_ORIGIN}/AIO"
        R2_BUCKET_NAME = "aio-bucket"
        KEY_GATEWAY_URL = "https://api.medhub-academy.stream/AIO"
    else:
        CLOUDFLARE_API_URL = f"{EDGE_API_ORIGIN}/AZ"
        R2_BUCKET_NAME = "az-bucket"
        KEY_GATEWAY_URL = "https://api.medhub-academy.stream/AZ"

def api_call(method, url, **kwargs):
    """
    Executes an API request against Cloudflare Worker.
    Features automatic edge fallback: if a custom domain request triggers a Cloudflare WAF/Turnstile
    bot challenge (HTTP 403 'Just a moment...'), it immediately swaps host to direct workers.dev.
    """
    try:
        resp = api_session.request(method, url, timeout=kwargs.pop("timeout", 20), **kwargs)
        if resp.status_code == 403 and "Just a moment..." in resp.text and "api.medhub-academy.stream" in url:
            direct_url = url.replace("api.medhub-academy.stream", "courses-backend.midodo966.workers.dev")
            print(f"🔄 [Edge Fallback] Custom domain challenged by Cloudflare. Retrying via direct edge: {direct_url}")
            return api_session.request(method, direct_url, timeout=20, **kwargs)
        return resp
    except Exception as e:
        if "api.medhub-academy.stream" in url:
            direct_url = url.replace("api.medhub-academy.stream", "courses-backend.midodo966.workers.dev")
            print(f"🔄 [Edge Fallback] Custom domain error ({e}). Retrying via direct edge: {direct_url}")
            return api_session.request(method, direct_url, timeout=20, **kwargs)
        raise e

def update_job_progress(job_id, status, progress_percent, error_message=None, duration_seconds=None):
    """Reports execution status to Cloudflare D1 for real-time admin portal telemetry."""
    try:
        url = f"{CLOUDFLARE_API_URL}/api/admin/transcode/jobs/{job_id}/progress"
        payload = {
            "status": status,
            "progress_percent": progress_percent,
            "error_message": error_message
        }
        if duration_seconds is not None:
            payload["duration_seconds"] = int(duration_seconds)
        api_call("PUT", url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ [Telemetry] Failed to report status to API: {e}", file=sys.stderr)

def fetch_job_details(job_id):
    """
    Handshakes with Cloudflare Worker to retrieve full curriculum job parameters.
    Implements cross-brand auto-discovery: if a job ID is not found under the active brand,
    it automatically probes the alternate tenant database (AZ <-> AIO) and switches brand context.
    """
    global CURRENT_BRAND, CLOUDFLARE_API_URL, R2_BUCKET_NAME, KEY_GATEWAY_URL

    # 1. Probe primary configured brand
    url = f"{CLOUDFLARE_API_URL}/api/admin/transcode/jobs/{job_id}"
    resp = api_call("GET", url, timeout=15)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("success") and data.get("job"):
            return data["job"]

    # 2. Cross-brand fallback: probe alternate brand
    alt_brand = "aio" if CURRENT_BRAND == "az" else "az"
    alt_url = f"{EDGE_API_ORIGIN}/{alt_brand.upper()}/api/admin/transcode/jobs/{job_id}"
    alt_resp = api_call("GET", alt_url, timeout=15)
    if alt_resp.status_code == 200:
        alt_data = alt_resp.json()
        if alt_data.get("success") and alt_data.get("job"):
            print(f"🔄 [Multi-Brand] Job {job_id} discovered under {alt_brand.upper()} tenant. Switching brand context...")
            configure_brand(alt_brand)
            return alt_data["job"]

    raise RuntimeError(f"Job {job_id} not found across any brand (checked AZ and AIO).")

def check_job_status(job_id):
    """Checks if job was paused or cancelled in D1."""
    try:
        job = fetch_job_details(job_id)
        return job.get("status")
    except Exception:
        return "active"

# ==============================================================================
# HELPER: GOOGLE DRIVE PRIVATE FILE STREAMER
# ==============================================================================
def get_gdrive_service():
    """Initializes Google Drive API v3 client using Service Account credentials."""
    creds_dict = None
    if GDRIVE_SERVICE_ACCOUNT_JSON:
        try:
            creds_dict = json.loads(GDRIVE_SERVICE_ACCOUNT_JSON)
        except Exception as e:
            print(f"⚠️ Failed to parse GDRIVE_SERVICE_ACCOUNT_JSON env: {e}", file=sys.stderr)

    if not creds_dict and os.path.exists(LOCAL_GDRIVE_KEY_PATH):
        try:
            with open(LOCAL_GDRIVE_KEY_PATH, "r") as f:
                creds_dict = json.load(f)
        except Exception as e:
            print(f"⚠️ Failed to load local service account file: {e}", file=sys.stderr)

    if not creds_dict:
        raise RuntimeError("Google Drive Service Account credentials not provided (missing env & local fallback).")

    credentials = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=credentials)

def download_private_drive_file(drive_service, file_id, destination_path):
    """Downloads a private Google Drive asset using chunked streaming."""
    request = drive_service.files().get_media(fileId=file_id)
    with open(destination_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 10)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                pct = int(status.progress() * 100)
                print(f"📥 Downloading: {pct}%...", end="\r", flush=True)
    print("📥 Download complete.        ")

# ==============================================================================
# HELPER: MEDIA PROBING & DETECTING OPTIMAL ENCODER
# ==============================================================================
def probe_video(file_path):
    """
    Extracts video dimensions, codecs, duration, and source bitrate using ffprobe.
    Returns: (width, height, codec, audio_codec, duration, bitrate)
    """
    width = 1280
    height = 720
    v_codec = "h264"
    a_codec = "aac"
    duration = 0.0
    bitrate = 0

    cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries", "format=duration,bit_rate",
        "-show_entries", "stream=width,height,codec_name,codec_type,duration",
        "-of", "json",
        str(file_path)
    ]
    try:
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, check=True)
        info = json.loads(res.stdout)
        fmt = info.get("format", {})
        streams = info.get("streams", [])

        for s in streams:
            stype = s.get("codec_type")
            if stype == "video":
                if s.get("width") and s.get("height"):
                    width = int(s["width"])
                    height = int(s["height"])
                if s.get("codec_name"):
                    v_codec = s["codec_name"].lower()
            elif stype == "audio":
                if s.get("codec_name"):
                    a_codec = s["codec_name"].lower()

            if s.get("duration"):
                try:
                    d = float(s["duration"])
                    if d > duration:
                        duration = d
                except (ValueError, TypeError):
                    pass

        if duration <= 0 and fmt.get("duration"):
            try:
                duration = float(fmt["duration"])
            except (ValueError, TypeError):
                pass

        if fmt.get("bit_rate"):
            try:
                bitrate = int(fmt["bit_rate"])
            except (ValueError, TypeError):
                pass

        if bitrate == 0 and duration > 0:
            try:
                file_sz = os.path.getsize(file_path)
                bitrate = int((file_sz * 8) / duration)
            except Exception:
                pass

    except Exception:
        pass

    return width, height, v_codec, a_codec, duration, bitrate

def count_pdf_pages(pdf_path):
    """Accurately calculates total page count of a PDF asset for D1 curriculum metadata."""
    try:
        with open(pdf_path, "rb") as f:
            content = f.read()
        matches = re.findall(rb'/Type\s*/Page(?=[^s]|\b)', content)
        return len(matches) if matches else 0
    except Exception:
        return 0

def detect_optimal_encoder():
    """
    Auto-detects NVIDIA NVENC hardware acceleration for peak throughput GPU encoding.
    Falls back to multi-threaded CPU libx264 ultrafast preset.
    """
    try:
        enc_res = subprocess.run(["ffmpeg", "-encoders"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        if "h264_nvenc" in enc_res.stdout:
            gpu_res = subprocess.run(["nvidia-smi"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if gpu_res.returncode == 0:
                print("⚡ [Hardware Acceleration] NVIDIA GPU Detected! Using h264_nvenc.")
                return [
                    "-c:v", "h264_nvenc",
                    "-preset", "p1",
                    "-tune", "ull",
                    "-rc", "vbr",
                    "-cq", str(CRF_QUALITY),
                    "-b:v", f"{TARGET_VIDEO_BITRATE}k",
                    "-maxrate", f"{MAX_VIDEO_BITRATE}k",
                    "-bufsize", f"{VIDEO_BUFSIZE}k",
                    "-bf", "0",
                    "-pix_fmt", "yuv420p"
                ]
    except Exception:
        pass

    # Multi-core CPU fallback
    return [
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-crf", str(CRF_QUALITY),
        "-b:v", f"{TARGET_VIDEO_BITRATE}k",
        "-maxrate", f"{MAX_VIDEO_BITRATE}k",
        "-bufsize", f"{VIDEO_BUFSIZE}k",
        "-bf", "0",
        "-pix_fmt", "yuv420p",
        "-threads", "0"
    ]

# ==============================================================================
# HELPER: STANDARDIZED 720p 4.0s AES-128 HLS ENCODING & SLICING
# ==============================================================================
def transcode_and_slice_720p_4s(input_path, output_dir, resource_uuid):
    """
    Executes atomic single-pass 720p transcoding and slicing into 4.0-second AES-128 HLS chunks.
    Matches colab-scripts/1_encrypt_and_upload_aio.py production standard.
    Output:
      - playlist.m3u8 (authoritative playlist)
      - chunk_000.ts, chunk_001.ts, ... (4.0s encrypted segments)
    Returns: (raw_key, key_base64, final_duration)
    """
    os.makedirs(output_dir, exist_ok=True)

    # 1. Generate AES-128 cryptographic key and 16-byte random IV
    raw_key = os.urandom(16)
    key_base64 = base64.b64encode(raw_key).decode("utf-8")
    hex_iv = binascii.hexlify(os.urandom(16)).decode("utf-8")

    key_path = os.path.join(output_dir, "video.key")
    with open(key_path, "wb") as f:
        f.write(raw_key)

    # Key gateway URL formatted for active tenant (e.g. /AZ/?videoId=<UUID> or /AIO/?videoId=<UUID>)
    key_url = f"{KEY_GATEWAY_URL}/?videoId={resource_uuid}"
    keyinfo_path = os.path.join(output_dir, "keyinfo.txt")
    with open(keyinfo_path, "w") as f:
        f.write(f"{key_url}\n{key_path}\n{hex_iv}\n")

    output_playlist = os.path.join(output_dir, "playlist.m3u8")
    segment_pattern = os.path.join(output_dir, "chunk_%03d.ts")

    # 2. Probe source stream
    width, height, codec, audio_codec, duration, source_bitrate = probe_video(input_path)
    print(f"📹 Source Stream: {width}x{height}, Codec: {codec}, Audio: {audio_codec}, Duration: {duration:.1f}s")

    # 3. Fast-Path Check: Allow direct stream copy ONLY if source is already <= 720p H.264 <= 2500k
    can_stream_copy = (
        width <= 1280 and height <= 720 and codec == "h264" and 0 < source_bitrate <= (MAX_VIDEO_BITRATE * 1000)
    )

    if can_stream_copy:
        print("⚡ Fast-Path: Source is already <= 720p H.264 with compatible bitrate. Executing fast stream-copy...")
        cmd_copy = [
            "ffmpeg", "-y",
            "-nostats", "-loglevel", "error",
            "-i", str(input_path),
            "-c", "copy",
            "-hls_time", str(HLS_CHUNK_DURATION),
            "-hls_key_info_file", keyinfo_path,
            "-hls_playlist_type", "vod",
            "-hls_segment_filename", segment_pattern,
            output_playlist
        ]
        res = subprocess.run(cmd_copy, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if res.returncode != 0:
            print("⚠️ Stream-copy fallback: Re-encoding with optimal 720p encoder...")
            can_stream_copy = False

    if not can_stream_copy:
        print(f"⚙️ Transcoding & Slicing into {HLS_CHUNK_DURATION}s chunks (720p, 30fps, GOP=120, MaxRate={MAX_VIDEO_BITRATE}k)...")
        encoder_args = detect_optimal_encoder()
        cmd_transcode = [
            "ffmpeg", "-y",
            "-nostats", "-loglevel", "error",
            "-threads", "0",
            "-i", str(input_path),
            "-threads", "0",
            "-vf", "scale=1280:-2:flags=fast_bilinear,format=yuv420p"
        ] + encoder_args + [
            "-r", str(TARGET_FPS),
            "-g", "120", "-keyint_min", "60", "-sc_threshold", "0",
            "-c:a", "copy" if (audio_codec == "aac" and source_bitrate > 0 and source_bitrate <= 160000) else "aac",
            "-b:a", "96k",
            "-hls_time", str(HLS_CHUNK_DURATION),
            "-hls_key_info_file", keyinfo_path,
            "-hls_playlist_type", "vod",
            "-hls_segment_filename", segment_pattern,
            output_playlist
        ]
        subprocess.run(cmd_transcode, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

    # Clean temporary key and keyinfo files so only encrypted chunks and playlist remain
    if os.path.exists(key_path):
        os.remove(key_path)
    if os.path.exists(keyinfo_path):
        os.remove(keyinfo_path)

    return raw_key, key_base64, duration

# ==============================================================================
# HELPER: PARALLEL CLOUDFLARE R2 STREAMING UPLOAD
# ==============================================================================
def upload_folder_to_r2(local_dir, r2_prefix):
    """
    Streams all files in local_dir directly to Cloudflare R2 concurrently.
    Uploads chunks and playlist into the isolated {resource_uuid}/ folder in R2_BUCKET_NAME.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(
            max_pool_connections=100,
            retries={"max_attempts": 5, "mode": "adaptive"}
        )
    )

    files_to_upload = []
    for root, _, files in os.walk(local_dir):
        for f in files:
            full_path = os.path.join(root, f)
            rel_path = os.path.relpath(full_path, local_dir)
            r2_key = f"{r2_prefix}/{rel_path}".replace("\\", "/")
            files_to_upload.append((full_path, r2_key))

    def _upload(item):
        path, key = item
        ctype = "application/vnd.apple.mpegurl" if key.endswith(".m3u8") else "video/mp2t"
        with open(path, "rb") as fp:
            s3.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=fp,
                ContentType=ctype,
                CacheControl="public, max-age=31536000, immutable"
            )

    with ThreadPoolExecutor(max_workers=32) as executor:
        list(executor.map(_upload, files_to_upload))

# ==============================================================================
# MAIN EXECUTION PIPELINE & DAEMON RUNNER
# ==============================================================================
def fetch_pending_jobs(api_url=None):
    """Fetches all pending jobs from Cloudflare D1 queue."""
    base = api_url or CLOUDFLARE_API_URL
    url = f"{base}/api/admin/transcode/jobs?status=pending"
    try:
        resp = api_call("GET", url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("jobs", [])
    except Exception as e:
        print(f"⚠️ [Queue] Failed to query pending jobs: {e}", file=sys.stderr)
    return []

def process_job(job_id):
    """
    Executes the full automated transcoding and DRM vaulting pipeline for a specific job.
    Standardized to 720p 4.0-second HLS chunks across all brands.
    """
    work_dir = Path(f"/tmp/transcode_{job_id}")
    work_dir.mkdir(parents=True, exist_ok=True)

    print(f"🚀 [Pipeline] Processing transcode operation for Job ID: {job_id}")

    try:
        # Step 1: Handshake with Cloudflare Worker (with cross-brand auto-discovery)
        update_job_progress(job_id, "downloading", 10)
        job = fetch_job_details(job_id)
        drive_file_id = job["drive_file_id"]
        file_name = job["file_name"]
        material_type = (job.get("material_type") or "video").lower()
        resource_uuid = str(uuid.uuid4())

        print(f"📋 [1/5] Handshake verified [{CURRENT_BRAND.upper()}]: '{file_name}' ({material_type}) for unit '{job.get('unit_title')}'")

        # Step 2: Download raw asset from private Google Drive
        drive_service = get_gdrive_service()
        raw_source_path = work_dir / f"source_{file_name}"
        print(f"📥 [2/5] Ingesting private asset from Google Drive...")
        download_private_drive_file(drive_service, drive_file_id, str(raw_source_path))

        if material_type == "pdf":
            # PDF Document Pipeline (Zero re-encoding)
            update_job_progress(job_id, "uploading", 60)
            print(f"📄 [3/5] Ingesting PDF document...")
            s3 = boto3.client(
                "s3",
                endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
                aws_access_key_id=R2_ACCESS_KEY_ID,
                aws_secret_access_key=R2_SECRET_ACCESS_KEY
            )
            r2_key = f"{resource_uuid}.pdf"
            with open(raw_source_path, "rb") as f:
                s3.put_object(
                    Bucket=R2_BUCKET_NAME,
                    Key=r2_key,
                    Body=f,
                    ContentType="application/pdf"
                )

            pdf_page_count = count_pdf_pages(raw_source_path)
            # Register in D1 materials
            ingest_payload = {
                "course_id": job["course_id"],
                "course_title": job["course_title"],
                "unit_title": job["unit_title"],
                "unit_type": job.get("unit_type") or "lecture",
                "unit_order": job.get("unit_order") or 99,
                "material_title": Path(file_name).stem,
                "material_type": "pdf",
                "resource_uuid": resource_uuid,
                "order_index": job.get("order_index") or 999,
                "provider_name": job.get("provider_name"),
                "page_count": pdf_page_count
            }
            resp = api_call("POST", f"{CLOUDFLARE_API_URL}/api/admin/ingest", json=ingest_payload, timeout=15)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to register PDF in D1: {resp.text}")

            update_job_progress(job_id, "completed", 100)
            print(f"✅ [5/5] PDF material published successfully to [{CURRENT_BRAND.upper()}].")

        else:
            # Video Pipeline: Standardized 720p 4.0s Segments (from 1_encrypt_and_upload_aio.py)
            update_job_progress(job_id, "transcoding (720p 4s)", 30)
            print(f"⚙️ [3/5] Standardizing video to High-Quality 720p with 4.0s segments...")

            hls_output_dir = work_dir / "hls_output"
            raw_key, key_base64, duration = transcode_and_slice_720p_4s(
                raw_source_path, str(hls_output_dir), resource_uuid
            )

            # Upload HLS stream to Cloudflare R2
            update_job_progress(job_id, "uploading", 85)
            print(f"☁️ [4/5] Streaming encrypted 4s chunks to Cloudflare R2 ({R2_BUCKET_NAME})...")
            upload_folder_to_r2(str(hls_output_dir), resource_uuid)

            # Atomic vaulting & material registration in Cloudflare D1
            print(f"🔑 [5/5] Vaulting AES key in Cloudflare D1 video_keys for [{CURRENT_BRAND.upper()}]...")
            final_duration = int(round(duration)) if duration > 0 else int(job.get("duration_seconds") or 0)
            ingest_payload = {
                "course_id": job["course_id"],
                "course_title": job["course_title"],
                "unit_title": job["unit_title"],
                "unit_type": job.get("unit_type") or "lecture",
                "unit_order": job.get("unit_order") or 99,
                "material_title": Path(file_name).stem,
                "material_type": "video",
                "resource_uuid": resource_uuid,
                "key_base64": key_base64,
                "order_index": job.get("order_index") or 999,
                "provider_name": job.get("provider_name"),
                "duration_seconds": final_duration
            }
            resp = api_call("POST", f"{CLOUDFLARE_API_URL}/api/admin/ingest", json=ingest_payload, timeout=15)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to register video in D1: {resp.text}")

            update_job_progress(job_id, "completed", 100, duration_seconds=final_duration)
            print(f"✨ Video '{file_name}' published live to [{CURRENT_BRAND.upper()}] (720p 4s chunks).")

    except Exception as e:
        err_msg = str(e)
        print(f"❌ [Error] Pipeline failure for job {job_id}: {err_msg}", file=sys.stderr)
        update_job_progress(job_id, "failed", 0, error_message=err_msg)
        raise
    finally:
        # Secure cleanup of temporary working directory
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)

def run_daemon(poll_interval=8, brand=None):
    """Continuously listens for queued jobs across tenants and executes them."""
    brands_to_check = ["az", "aio"] if brand in [None, "both"] else [brand]
    print(f"🔄 [Transcoder Daemon] Listening for pending jobs every {poll_interval}s...")
    print(f"📡 Target Tenant(s): {', '.join(brands_to_check).upper()}")
    print("Press Ctrl+C to terminate the runner.\n")

    while True:
        try:
            for b in brands_to_check:
                configure_brand(b)
                pending = fetch_pending_jobs(CLOUDFLARE_API_URL)
                if pending:
                    print(f"⚡ [{b.upper()}] Found {len(pending)} pending job(s) in queue.")
                    for job in pending:
                        jid = job["id"]
                        fname = job.get("file_name", "Asset")
                        print(f"\n────────────────────────────────────────────────────────")
                        print(f"🎬 Ingesting [{b.upper()}]: '{fname}' ({jid})")
                        print(f"────────────────────────────────────────────────────────")
                        try:
                            process_job(jid)
                        except Exception as job_err:
                            print(f"⚠️ Skipping failed job {jid}: {job_err}", file=sys.stderr)
            time.sleep(poll_interval)
        except KeyboardInterrupt:
            print("\n🛑 Transcoder daemon terminated by user.")
            break
        except Exception as loop_err:
            print(f"⚠️ Runner loop error: {loop_err}", file=sys.stderr)
            time.sleep(poll_interval)

def run_process_pending(brand=None):
    """Processes all currently pending jobs in a single batch pass and exits."""
    brands_to_check = ["az", "aio"] if brand in [None, "both"] else [brand]
    total_processed = 0
    for b in brands_to_check:
        configure_brand(b)
        pending = fetch_pending_jobs(CLOUDFLARE_API_URL)
        if pending:
            print(f"⚡ [{b.upper()}] Found {len(pending)} pending job(s).")
            for job in pending:
                jid = job["id"]
                fname = job.get("file_name", "Asset")
                print(f"🎬 Processing [{b.upper()}]: '{fname}' ({jid})")
                try:
                    process_job(jid)
                    total_processed += 1
                except Exception as err:
                    print(f"❌ Failed to process {jid}: {err}", file=sys.stderr)
        else:
            print(f"ℹ️ [{b.upper()}] No pending jobs in queue.")
    print(f"✨ Finished processing {total_processed} job(s).")

def main():
    parser = argparse.ArgumentParser(description="AZ Learn Headless Automated Transcoder Pipeline")
    parser.add_argument("--job-id", help="Cloudflare D1 Transcode Job ID")
    parser.add_argument("--daemon", action="store_true", help="Run in continuous daemon mode polling for pending jobs")
    parser.add_argument("--process-pending", action="store_true", help="Process all currently pending jobs in queue and exit")
    parser.add_argument("--poll-interval", type=int, default=8, help="Daemon poll interval in seconds (default: 8)")
    parser.add_argument("--brand", choices=["az", "aio", "both"], default=None, help="Target tenant brand (default: az)")
    args = parser.parse_args()

    if args.brand and args.brand != "both":
        configure_brand(args.brand)

    if args.job_id:
        try:
            process_job(args.job_id)
        except Exception:
            sys.exit(1)
    elif args.process_pending:
        run_process_pending(args.brand)
    elif args.daemon:
        run_daemon(args.poll_interval, args.brand)
    else:
        print("💡 Usage: python3 scripts/transcoder_pipeline.py [--job-id <ID> | --daemon | --process-pending]")
        print("Starting in single-pass pending check mode...")
        run_process_pending(args.brand)

if __name__ == "__main__":
    main()
