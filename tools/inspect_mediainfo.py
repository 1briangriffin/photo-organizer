#!/usr/bin/env python3
"""
Inspect all available MediaInfo fields for video files to identify camera model fields.

Usage:
  python tools/inspect_mediainfo.py <video_file>

Example:
  python tools/inspect_mediainfo.py "path/to/video.mp4"
"""

import sys
from pathlib import Path

try:
    from pymediainfo import MediaInfo
except ImportError:
    print("Error: pymediainfo not installed")
    sys.exit(1)


def inspect_file(video_path):
    """Dump all MediaInfo attributes for inspection."""
    path = Path(video_path)
    if not path.exists():
        print(f"Error: File not found: {video_path}")
        sys.exit(1)

    print(f"Inspecting: {path.name}\n")
    
    try:
        mi = MediaInfo.parse(str(path))
    except Exception as e:
        print(f"Error parsing: {e}")
        sys.exit(1)

    # First, show camera-related fields if any
    print("="*70)
    print("CAMERA-RELATED FIELDS (if present)")
    print("="*70)
    camera_keywords = {
        'performer', 'device_model', 'encoder', 'make', 'model', 'camera',
        'product', 'device', 'equipment_model', 'host_computer'
    }
    found_any = False
    for track in mi.tracks:
        attrs = [attr for attr in dir(track) if not attr.startswith('_') and not callable(getattr(track, attr))]
        for attr in attrs:
            if any(kw in attr.lower() for kw in camera_keywords):
                val = getattr(track, attr, None)
                if val is not None and val != "":
                    print(f"  [{track.track_type}] {attr}: {val}")
                    found_any = True
    if not found_any:
        print("  (no camera-related metadata found)")

    print("\n" + "="*70)
    print("ALL METADATA BY TRACK")
    print("="*70)

    for i, track in enumerate(mi.tracks):
        print(f"\nTrack {i}: {track.track_type}")
        print("-" * 70)
        
        # Get all attributes, excluding private ones and methods
        attrs = [attr for attr in dir(track) if not attr.startswith('_') and not callable(getattr(track, attr))]
        
        for attr in sorted(attrs):
            val = getattr(track, attr, None)
            if val is not None and val != "":
                # Truncate long values for readability
                val_str = str(val)
                if len(val_str) > 60:
                    val_str = val_str[:57] + "..."
                print(f"  {attr}: {val_str}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python inspect_mediainfo.py <video_file>")
        sys.exit(1)
    inspect_file(sys.argv[1])
