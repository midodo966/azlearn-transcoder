# AZ Learn Automated Transcoder Runner

Headless DRM video transcoding and multi-bitrate HLS encryption runner for the AZ Learn / All In One learning platform.

## Architectural Overview
- Ingests source video streams directly from Google Drive API v3.
- Builds multi-rendition ladder (1080p, 720p, 480p) via FFmpeg.
- Encrypts and slices video into 8.0-second AES-128 HLS chunks.
- Streams encrypted assets directly to Cloudflare R2 storage.
- Atomically vaults encryption keys in Cloudflare D1 serverless database.
- Real-time execution telemetry reported back to Cloudflare D1 queue.
