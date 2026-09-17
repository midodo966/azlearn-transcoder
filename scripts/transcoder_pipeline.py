#!/usr/bin/env python3
"""
================================================================================
AZ LEARN (MED HUB) — AUTOMATED HEADLESS TRANSCODING & DRM INGESTION PIPELINE
================================================================================
Architectural Overview & OpSec Principles:
1. 100% Private Google Drive Ingestion (Restricted Access):
   - Authenticates directly with Google Drive API v3 using Google Cloud Service Account
     credentials (drivebot@sodium-ray-508905-a4.iam.gserviceaccount.com).
   - Zero public sharing links required; tutors upload directly to private subfolders.

2. Hardware-Optimized Multi-Rendition Transcoding (HandBrake / FFmpeg):
   - Generates adaptive multi-bitrate ladder (1080p, 720p, 480p) tailored to source stream.
   - Uses HandBrakeCLI when available with constant framerate (-r 30 --cfr),
     falling back to high-throughput ffmpeg veryfast preset.

3. Stealth AES-128 Cryptographic DRM (Rule 13 Compliance):
   - Generates single 16-byte random key and 16-byte IV per video asset.
   - Segments into 8.0-second HLS chunks with master playlist linking quality variants.
   - Interceptors fetch AES keys exclusively through Cloudflare Worker key gate.

4. Direct High-Speed Cloudflare R2 Edge Streaming (Rule 17 Compliance):
   - Pushes all chunks concurrently via Boto3 with 100-connection connection pool.
   - Zero Workers media proxying; served directly via dedicated R2 Custom Domain.

5. Atomic D1 Vaulting & Execution Telemetry:
   - Vaults AES-128 key directly into Cloudflare D1 video_keys table via POST /api/admin/ingest.
   - Reports live progress percentages (10%, 30%, 75%, 100%) to Cloudflare D1 transcode_jobs.
================================================================================
"""

import os
import sys
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
CLOUDFLARE_API_URL = os.environ.get("CLOUDFLARE_API_URL", "https://api.medhub-academy.stream/AZ")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "azlearn_adm_sec_9f8b7c6d5e4a3b2c1d0e9f8a7b6c5d4e")

R2_ACCOUNT_ID = os.environ.get("R2_ACCOUNT_ID", "e7e407d343739d728e23cf0e0f815c87")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "a8054551b4797c7ddfa2f9c6415a9e02")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "bb3f5a77be861ed9a34e6f995a684cac244f0f392139f7fd459acf5f8e1ddc37")
R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "az-bucket")

# Local fallback path for Google Service Account credentials
LOCAL_GDRIVE_KEY_PATH = "/home/midododo/Downloads/sodium-ray-508905-a4-bf28edccfcde.json"
GDRIVE_SERVICE_ACCOUNT_JSON = os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON", "")

# ==============================================================================
# HELPER: TELEMETRY & WORKER API CLIENT
# ==============================================================================
api_session = requests.Session()
api_session.headers.update({
    "Authorization": f"Bearer {ADMIN_KEY}",
    "Content-Type": "application/json"
})

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
        api_session.put(url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ [Telemetry] Failed to report status to API: {e}", file=sys.stderr)

def fetch_job_details(job_id):
    """Handshakes with Cloudflare Worker to retrieve full curriculum job parameters."""
    url = f"{CLOUDFLARE_API_URL}/api/admin/transcode/jobs/{job_id}"
    resp = api_session.get(url, timeout=15)
    if resp.status_code != 200:
        raise RuntimeError(f"Handshake failed (HTTP {resp.status_code}): {resp.text}")
    data = resp.json()
    if not data.get("success") or not data.get("job"):
        raise RuntimeError(f"Job not found or invalid response: {data}")
    return data["job"]

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
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    if GDRIVE_SERVICE_ACCOUNT_JSON:
        try:
            creds_dict = json.loads(GDRIVE_SERVICE_ACCOUNT_JSON)
            credentials = service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/drive.readonly"]
            )
            return build("drive", "v3", credentials=credentials)
        except Exception as e:
            print(f"⚠️ Failed to parse GDRIVE_SERVICE_ACCOUNT_JSON: {e}", file=sys.stderr)

    if os.path.exists(LOCAL_GDRIVE_KEY_PATH):
        creds = service_account.Credentials.from_service_account_file(
            LOCAL_GDRIVE_KEY_PATH, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        return build("drive", "v3", credentials=creds)

    raise RuntimeError(f"Google Drive service account credentials not found in GDRIVE_SERVICE_ACCOUNT_JSON or at {LOCAL_GDRIVE_KEY_PATH}")

def download_private_drive_file(drive_service, file_id, output_path):
    """Streams a large binary file from Google Drive directly to disk."""
    from googleapiclient.http import MediaIoBaseDownload
    import io

    request = drive_service.files().get_media(fileId=file_id)
    with open(output_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request, chunksize=1024 * 1024 * 16)
        done = False
        while not done:
            status, done = downloader.next_chunk()

# ==============================================================================
# HELPER: VIDEO PROBING & MULTI-RENDITION TRANSCODING
# ==============================================================================
def probe_video(video_path):
    """Extracts width, height, and duration using ffprobe across stream and format headers."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "stream=width,height,duration:format=duration",
        "-of", "json",
        str(video_path)
    ]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    width = 1920
    height = 1080
    duration = 0.0
    try:
        info = json.loads(res.stdout)
        streams = info.get("streams", [])
        for s in streams:
            if s.get("width") and s.get("height"):
                width = int(s["width"])
                height = int(s["height"])
            if s.get("duration"):
                try:
                    d = float(s["duration"])
                    if d > duration:
                        duration = d
                except (ValueError, TypeError):
                    pass
        if duration <= 0 and "format" in info and info["format"].get("duration"):
            try:
                duration = float(info["format"]["duration"])
            except (ValueError, TypeError):
                pass
    except Exception:
        pass
    return width, height, duration

def count_pdf_pages(pdf_path):
    """Accurately calculates total page count of a PDF asset for D1 curriculum metadata."""
    try:
        import pypdf
        reader = pypdf.PdfReader(str(pdf_path))
        return len(reader.pages)
    except Exception:
        pass
    try:
        with open(pdf_path, "rb") as f:
            content = f.read()
        import re
        matches = re.findall(rb'/Type\s*/Page(?=[^s]|\b)', content)
        return len(matches) if matches else 0
    except Exception:
        return 0

def has_handbrake():
    """Checks if HandBrakeCLI binary is present in system PATH."""
    return shutil.which("HandBrakeCLI") is not None

def transcode_rendition(input_path, output_path, target_width, target_height, rf_quality, max_bitrate_k):
    """
    Transcodes a specific video rendition with HandBrakeCLI or optimized FFmpeg.
    Suppresses stdout/stderr to prevent log leaking (OpSec Principle 2).
    """
    if has_handbrake():
        cmd = [
            "HandBrakeCLI",
            "-i", str(input_path),
            "-o", str(output_path),
            "-e", "x264",
            "-q", str(rf_quality),
            "--encoder-preset", "fast",
            "--cfr", "-r", "30",
            "-w", str(target_width),
            "-l", str(target_height),
            "--loose-anamorphic",
            "-E", "av_aac",
            "-B", "96"
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    else:
        # High-performance ffmpeg fallback
        cmd = [
            "ffmpeg", "-y",
            "-loglevel", "error",
            "-i", str(input_path),
            "-vf", f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,pad=ceil(iw/2)*2:ceil(ih/2)*2,format=yuv420p",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", str(rf_quality),
            "-maxrate", f"{max_bitrate_k}k",
            "-bufsize", f"{max_bitrate_k * 2}k",
            "-r", "30",
            "-g", "60",
            "-keyint_min", "30",
            "-sc_threshold", "0",
            "-c:a", "aac",
            "-b:a", "96k",
            str(output_path)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

# ==============================================================================
# HELPER: AES-128 HLS SLICING & ENCRYPTION
# ==============================================================================
def slice_aes128_hls(renditions, output_dir, resource_uuid):
    """
    Slices video renditions into 8.0s AES-128 encrypted HLS segments.
    Creates stream playlists and Master playlist.m3u8 linking quality variants.
    Returns (raw_key_bytes, base64_key).
    """
    os.makedirs(output_dir, exist_ok=True)
    raw_key = os.urandom(16)
    key_base64 = base64.b64encode(raw_key).decode("utf-8")
    hex_iv = binascii.hexlify(os.urandom(16)).decode("utf-8")

    key_path = os.path.join(output_dir, "video.key")
    with open(key_path, "wb") as f:
        f.write(raw_key)

    keyinfo_path = os.path.join(output_dir, "keyinfo.txt")
    key_url = f"{CLOUDFLARE_API_URL}/?videoId={resource_uuid}"
    with open(keyinfo_path, "w") as f:
        f.write(f"{key_url}\n{key_path}\n{hex_iv}\n")

    master_lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3"
    ]

    for rendition in renditions:
        res_name = rendition["name"]
        stream_file = rendition["file"]
        variant_playlist = f"stream_{res_name}.m3u8"
        segment_pattern = f"chunk_{res_name}_%03d.ts"

        cmd = [
            "ffmpeg", "-y",
            "-loglevel", "error",
            "-i", str(stream_file),
            "-c", "copy",
            "-hls_time", "8.0",
            "-hls_key_info_file", keyinfo_path,
            "-hls_playlist_type", "vod",
            "-hls_segment_filename", os.path.join(output_dir, segment_pattern),
            os.path.join(output_dir, variant_playlist)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

        bandwidth = rendition["bandwidth"]
        width = rendition["width"]
        height = rendition["height"]
        master_lines.append(f"#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},RESOLUTION={width}x{height}")
        master_lines.append(variant_playlist)

    # Write authoritative master playlist
    master_path = os.path.join(output_dir, "playlist.m3u8")
    with open(master_path, "w") as f:
        f.write("\n".join(master_lines) + "\n")

    # Clean temporary key and keyinfo files so only encrypted chunks remain
    if os.path.exists(key_path):
        os.remove(key_path)
    if os.path.exists(keyinfo_path):
        os.remove(keyinfo_path)

    return raw_key, key_base64

# ==============================================================================
# HELPER: PARALLEL CLOUDFLARE R2 STREAMING UPLOAD
# ==============================================================================
def upload_folder_to_r2(local_dir, r2_prefix):
    """Streams all files in local_dir directly to Cloudflare R2 in parallel."""
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(max_pool_connections=50, retries={"max_attempts": 5, "mode": "adaptive"})
    )

    files_to_upload = []
    for root, _, files in os.walk(local_dir):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, local_dir)
            r2_key = f"{r2_prefix}/{rel_path}".replace("//", "/")
            files_to_upload.append((full_path, r2_key))

    def _upload(item):
        path, key = item
        content_type = "video/MP2T" if key.endswith(".ts") else ("application/x-mpegURL" if key.endswith(".m3u8") else "application/octet-stream")
        with open(path, "rb") as f:
            s3.put_object(
                Bucket=R2_BUCKET_NAME,
                Key=key,
                Body=f,
                ContentType=content_type,
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
        resp = api_session.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("jobs", [])
    except Exception as e:
        print(f"⚠️ [Queue] Failed to query pending jobs: {e}", file=sys.stderr)
    return []

def configure_brand(brand):
    """Dynamically adjusts API base and R2 bucket for multi-tenant deployments."""
    global CLOUDFLARE_API_URL, R2_BUCKET_NAME
    if brand == "aio":
        CLOUDFLARE_API_URL = os.environ.get("CLOUDFLARE_API_URL", "https://api.medhub-academy.stream/AIO")
        R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "aio-bucket")
    else:
        CLOUDFLARE_API_URL = os.environ.get("CLOUDFLARE_API_URL", "https://api.medhub-academy.stream/AZ")
        R2_BUCKET_NAME = os.environ.get("R2_BUCKET_NAME", "az-bucket")

def process_job(job_id):
    """Executes the full automated transcoding and DRM vaulting pipeline for a specific job."""
    work_dir = Path(f"/tmp/transcode_{job_id}")
    work_dir.mkdir(parents=True, exist_ok=True)

    print(f"🚀 [Pipeline] Processing transcode operation for Job ID: {job_id}")

    try:
        # Step 1: Handshake with Cloudflare Worker
        update_job_progress(job_id, "downloading", 10)
        job = fetch_job_details(job_id)
        drive_file_id = job["drive_file_id"]
        file_name = job["file_name"]
        material_type = (job.get("material_type") or "video").lower()
        resource_uuid = str(uuid.uuid4())

        print(f"📋 [1/5] Handshake verified: '{file_name}' ({material_type}) for unit '{job.get('unit_title')}'")

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
            resp = api_session.post(f"{CLOUDFLARE_API_URL}/api/admin/ingest", json=ingest_payload, timeout=15)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to register PDF in D1: {resp.text}")

            update_job_progress(job_id, "completed", 100)
            print("✅ [5/5] PDF material published successfully.")

        else:
            # Video Pipeline: Probe -> Multi-Rendition Transcode -> Slicing -> R2
            update_job_progress(job_id, "transcoding", 25)
            width, height, duration = probe_video(raw_source_path)
            print(f"⚙️ [3/5] Source detected: {width}x{height} ({duration:.1f}s). Building multi-rendition ladder...")

            rendition_configs = []
            if width >= 1920 or height >= 1080:
                rendition_configs.append({"name": "1080p", "width": 1920, "height": 1080, "rf": 22, "bitrate": 2800, "bandwidth": 3200000})
            if width >= 1280 or height >= 720 or not rendition_configs:
                rendition_configs.append({"name": "720p", "width": 1280, "height": 720, "rf": 24, "bitrate": 1500, "bandwidth": 1800000})
            rendition_configs.append({"name": "480p", "width": 854, "height": 480, "rf": 26, "bitrate": 800, "bandwidth": 950000})

            encoded_renditions = []
            total_renditions = len(rendition_configs)
            for idx, r in enumerate(rendition_configs):
                pct = 25 + int((idx / total_renditions) * 40)
                update_job_progress(job_id, f"transcoding ({r['name']})", pct)
                rendition_path = work_dir / f"encoded_{r['name']}.mp4"
                transcode_rendition(raw_source_path, rendition_path, r["width"], r["height"], r["rf"], r["bitrate"])
                encoded_renditions.append({
                    "name": r["name"],
                    "file": rendition_path,
                    "width": r["width"],
                    "height": r["height"],
                    "bandwidth": r["bandwidth"]
                })

            # Slice into 8.0s AES-128 HLS
            update_job_progress(job_id, "encrypting", 70)
            print(f"🔒 Slicing into 8.0s AES-128 HLS chunks...")
            hls_output_dir = work_dir / "hls_output"
            raw_key, key_base64 = slice_aes128_hls(encoded_renditions, str(hls_output_dir), resource_uuid)

            # Upload HLS stream to Cloudflare R2
            update_job_progress(job_id, "uploading", 85)
            print(f"☁️ [4/5] Streaming encrypted chunks to Cloudflare R2 ({R2_BUCKET_NAME})...")
            upload_folder_to_r2(str(hls_output_dir), resource_uuid)

            # Atomic vaulting & material registration in Cloudflare D1
            print(f"🔑 [5/5] Vaulting AES key in Cloudflare D1 video_keys...")
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
            resp = api_session.post(f"{CLOUDFLARE_API_URL}/api/admin/ingest", json=ingest_payload, timeout=15)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to register video in D1: {resp.text}")

            update_job_progress(job_id, "completed", 100, duration_seconds=final_duration)
            print(f"✨ Video '{file_name}' successfully transcoded, encrypted, and published live.")

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
                print(f"🎬 Processing: '{fname}' ({jid})")
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

    if args.brand:
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
        # Default behavior when run without flags: check if pending jobs exist, otherwise print usage
        print("💡 Usage: python3 scripts/transcoder_pipeline.py [--job-id <ID> | --daemon | --process-pending]")
        print("Starting in single-pass pending check mode...")
        run_process_pending(args.brand)

if __name__ == "__main__":
    main()
