#!/usr/bin/env python3
"""
Fetch latest NWM (National Water Model) data and upload to S3 as JSON.

This script:
1. Downloads the latest NWM channel routing NetCDF from NOAA S3
2. Extracts velocity and streamflow for each COMID
3. Categorizes flow into styling buckets
4. Uploads JSON to S3 for frontend consumption

Usage:
    python fetch_nwm.py [--dry-run]
"""

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import numpy as np
import requests
import xarray as xr
from dotenv import load_dotenv

load_dotenv()

# NOAA NWM S3 bucket (public, no auth needed)
NWM_BUCKET = "noaa-nwm-pds"
NWM_REGION = "us-east-1"

# Output S3 bucket (uses AWS_PROFILE from env for auth)
OUTPUT_BUCKET = os.getenv("S3_BUCKET_NAME", "nwm-streamflow-data")
OUTPUT_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_PROFILE = os.getenv("AWS_PROFILE")
OUTPUT_KEY = "live/current_velocity.json"


def get_latest_nwm_url() -> tuple[str, datetime]:
    """
    Find the most recent NWM analysis & assimilation file.
    
    NWM files are organized as:
    s3://noaa-nwm-pds/nwm.YYYYMMDD/analysis_assim/nwm.tHHz.analysis_assim.channel_rt.tm00.conus.nc
    
    Returns:
        Tuple of (s3_url, reference_time)
    """
    s3 = boto3.client("s3", region_name=NWM_REGION)
    s3._request_signer.sign = lambda *args, **kwargs: None  # Anonymous access
    
    # Try today and yesterday
    now = datetime.now(timezone.utc)
    
    for days_ago in range(2):
        date = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if days_ago:
            date = date.replace(day=date.day - days_ago)
        
        date_str = date.strftime("%Y%m%d")
        prefix = f"nwm.{date_str}/analysis_assim/"
        
        try:
            response = s3.list_objects_v2(
                Bucket=NWM_BUCKET,
                Prefix=prefix,
                MaxKeys=100
            )
            
            if "Contents" not in response:
                continue
            
            # Find channel_rt files (contain velocity and streamflow)
            channel_files = [
                obj["Key"] for obj in response["Contents"]
                if "channel_rt" in obj["Key"] and obj["Key"].endswith(".nc")
            ]
            
            if not channel_files:
                continue
            
            # Get the most recent hour
            channel_files.sort(reverse=True)
            latest = channel_files[0]
            
            # Parse the hour from filename (e.g., nwm.t12z.analysis_assim...)
            hour_str = latest.split(".t")[1][:2]
            ref_time = date.replace(hour=int(hour_str))
            
            url = f"s3://{NWM_BUCKET}/{latest}"
            print(f"Found latest NWM file: {latest}")
            print(f"Reference time: {ref_time.isoformat()}")
            
            return url, ref_time
            
        except Exception as e:
            print(f"Error checking {date_str}: {e}")
            continue
    
    raise RuntimeError("Could not find recent NWM data")


def download_nwm_file(s3_url: str) -> Path:
    """Download NWM NetCDF file to temp directory."""
    # Parse S3 URL
    parts = s3_url.replace("s3://", "").split("/", 1)
    bucket = parts[0]
    key = parts[1]
    
    # Use HTTPS URL for anonymous download
    https_url = f"https://{bucket}.s3.amazonaws.com/{key}"
    
    print(f"Downloading from: {https_url}")
    
    # Download to temp file
    temp_dir = Path(tempfile.mkdtemp())
    temp_file = temp_dir / "nwm_channel_rt.nc"
    
    response = requests.get(https_url, stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get("content-length", 0))
    downloaded = 0
    
    with open(temp_file, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = (downloaded / total_size) * 100
                print(f"\rDownloading: {pct:.1f}%", end="", flush=True)
    
    print(f"\nDownloaded {downloaded / 1024 / 1024:.1f} MB to {temp_file}")
    return temp_file


def categorize_velocity(velocity_ms: float) -> str:
    """Categorize velocity into styling buckets."""
    if velocity_ms < 0.1:
        return "very_slow"
    elif velocity_ms < 0.3:
        return "slow"
    elif velocity_ms < 0.6:
        return "moderate"
    elif velocity_ms < 1.0:
        return "fast"
    elif velocity_ms < 2.0:
        return "very_fast"
    else:
        return "extreme"


def categorize_streamflow(cms: float) -> str:
    """Categorize streamflow into styling buckets."""
    if cms < 1:
        return "very_low"
    elif cms < 10:
        return "low"
    elif cms < 50:
        return "moderate"
    elif cms < 200:
        return "high"
    elif cms < 1000:
        return "very_high"
    else:
        return "extreme"


def process_nwm_data(nc_path: Path, min_streamflow: float = 10.0) -> dict:
    """
    Process NWM NetCDF and extract streamflow per COMID.
    
    Args:
        nc_path: Path to NetCDF file
        min_streamflow: Minimum streamflow (m³/s) to include (filters out tiny streams)
    
    Returns dict ready for JSON serialization.
    Output format: {comid: streamflow_cms} — minimal for smallest JSON size
    """
    print(f"Processing NetCDF: {nc_path}")
    
    ds = xr.open_dataset(nc_path)
    
    # NWM channel_rt contains:
    # - feature_id: COMID (NHDPlus reach identifier)
    # - streamflow: discharge in m³/s
    # - velocity: water velocity in m/s
    
    comids = ds["feature_id"].values
    streamflow = ds["streamflow"].values.flatten()
    
    print(f"Processing {len(comids):,} reaches (min flow: {min_streamflow} m³/s)...")
    
    # MINIMAL format: {comid: streamflow_cms}
    # Frontend can categorize/style based on value
    sites = {}
    skipped = 0
    
    for i, comid in enumerate(comids):
        q = float(streamflow[i])
        
        # Skip invalid/missing data
        if np.isnan(q) or q < 0:
            skipped += 1
            continue
        
        # Filter out tiny streams
        if q < min_streamflow:
            skipped += 1
            continue
        
        # Just comid → flow (rounded to 2 decimals)
        sites[str(comid)] = round(q, 2)
    
    ds.close()
    
    print(f"Extracted data for {len(sites):,} reaches (skipped {skipped:,})")
    
    return sites


def upload_to_s3(data: dict, ref_time: datetime, dry_run: bool = False) -> str:
    """Upload JSON to S3."""
    
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "reference_time": ref_time.isoformat(),
        "site_count": len(data),
        "sites": data,
    }
    
    json_str = json.dumps(output, separators=(",", ":"))  # Compact JSON
    json_bytes = json_str.encode("utf-8")
    
    size_mb = len(json_bytes) / 1024 / 1024
    print(f"JSON size: {size_mb:.2f} MB")
    
    if dry_run:
        print(f"[DRY RUN] Would upload to s3://{OUTPUT_BUCKET}/{OUTPUT_KEY}")
        # Save locally for inspection
        local_path = Path("current_velocity.json")
        with open(local_path, "w") as f:
            json.dump(output, f, indent=2)
        print(f"Saved to {local_path}")
        return str(local_path)
    
    # Create session with profile if specified, otherwise use default credentials
    if AWS_PROFILE:
        print(f"Using AWS profile: {AWS_PROFILE}")
        session = boto3.Session(profile_name=AWS_PROFILE)
        s3 = session.client("s3", region_name=OUTPUT_REGION)
    else:
        s3 = boto3.client("s3", region_name=OUTPUT_REGION)
    
    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=OUTPUT_KEY,
        Body=json_bytes,
        ContentType="application/json",
        CacheControl="max-age=300",  # 5 minute cache
    )
    
    url = f"https://{OUTPUT_BUCKET}.s3.{OUTPUT_REGION}.amazonaws.com/{OUTPUT_KEY}"
    print(f"Uploaded to: {url}")
    
    return url


def main():
    parser = argparse.ArgumentParser(description="Fetch NWM data and upload to S3")
    parser.add_argument("--dry-run", action="store_true", help="Don't upload, save locally")
    args = parser.parse_args()
    
    print("=" * 60)
    print("NWM to S3 Pipeline")
    print("=" * 60)
    
    # 1. Find latest NWM file
    s3_url, ref_time = get_latest_nwm_url()
    
    # 2. Download NetCDF
    nc_path = download_nwm_file(s3_url)
    
    try:
        # 3. Process data
        sites = process_nwm_data(nc_path)
        
        # 4. Upload to S3
        output_url = upload_to_s3(sites, ref_time, dry_run=args.dry_run)
        
        print("=" * 60)
        print("SUCCESS!")
        print(f"Output: {output_url}")
        print("=" * 60)
        
    finally:
        # Cleanup temp file
        if nc_path.exists():
            nc_path.unlink()
            nc_path.parent.rmdir()


if __name__ == "__main__":
    main()
