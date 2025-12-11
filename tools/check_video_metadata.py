#!/usr/bin/env python3
"""
Diagnostic tool to compare video metadata extraction across pymediainfo, ffprobe, and exiftool.

Usage:
  python tools/check_video_metadata.py <sample_dir> [<output_csv>]

Example:
  python tools/check_video_metadata.py "D:\\Videos" video_report.csv
  uv run python tools/check_video_metadata.py . video_report.csv

Output:
  CSV file with columns: file, tool, capture_datetime, duration_sec, width, height, codec, camera_model, notes
  One row per file-tool combination.
"""

import csv
import json
import subprocess
import sys
from pathlib import Path

try:
    from pymediainfo import MediaInfo
except ImportError:
    MediaInfo = None


def media_info_extract(path):
    """Extract metadata using pymediainfo.MediaInfo.parse()."""
    if MediaInfo is None:
        return {"error": "pymediainfo not installed"}
    try:
        mi = MediaInfo.parse(str(path))
        data = {}
        for track in mi.tracks:
            if track.track_type == "General":
                # Duration is in milliseconds; convert to seconds
                if hasattr(track, "duration") and track.duration:
                    data["duration_sec"] = float(track.duration) / 1000.0
                # Try various date fields
                for field in ["recorded_date", "encoded_date", "tagged_date", "file_last_modification_date"]:
                    if hasattr(track, field) and getattr(track, field):
                        data["capture_datetime"] = getattr(track, field)
                        break
                # Camera model from performer (device/manufacturer), encoder, or device_model
                for field in ["performer", "encoder", "device_model"]:
                    if hasattr(track, field) and getattr(track, field):
                        data["camera_model"] = getattr(track, field)
                        break
            elif track.track_type == "Video":
                if hasattr(track, "width") and track.width:
                    data["width"] = track.width
                if hasattr(track, "height") and track.height:
                    data["height"] = track.height
                if hasattr(track, "format") and track.format:
                    data["codec"] = track.format
        return data
    except Exception as e:
        return {"error": str(e)}


def ffprobe_extract(path):
    """Extract metadata using ffprobe (FFmpeg)."""
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True)
        j = json.loads(out)
    except FileNotFoundError:
        return {"error": "ffprobe not found on PATH"}
    except subprocess.CalledProcessError as e:
        return {"error": f"ffprobe error: {e}"}
    except Exception as e:
        return {"error": str(e)}

    data = {}
    fmt = j.get("format", {})

    # Duration from format
    if fmt.get("duration"):
        data["duration_sec"] = float(fmt["duration"])

    # Try to find creation_time in tags (format or streams)
    creation = None
    for source in [fmt] + j.get("streams", []):
        tags = source.get("tags", {}) or {}
        if not creation:
            creation = (
                tags.get("creation_time")
                or tags.get("com.apple.quicktime.creationdate")
                or tags.get("date")
            )
    if creation:
        data["capture_datetime"] = creation

    # Take first video stream's properties
    for stream in j.get("streams", []):
        if stream.get("codec_type") == "video":
            if stream.get("width"):
                data["width"] = stream["width"]
            if stream.get("height"):
                data["height"] = stream["height"]
            if stream.get("codec_name"):
                data["codec"] = stream["codec_name"]
            break

    return data


def exiftool_extract(path):
    """Extract metadata using exiftool."""
    cmd = ["exiftool", "-j", str(path)]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True)
        arr = json.loads(out)
        if not arr:
            return {}
    except FileNotFoundError:
        return {"error": "exiftool not found on PATH"}
    except subprocess.CalledProcessError as e:
        return {"error": f"exiftool error: {e}"}
    except Exception as e:
        return {"error": str(e)}

    tagmap = arr[0] if arr else {}
    data = {}

    # Datetime
    for field in ["CreateDate", "CreationDate", "DateTimeOriginal", "FileModifyDate"]:
        if tagmap.get(field):
            data["capture_datetime"] = tagmap[field]
            break

    # Duration (may be string, e.g. "00:00:05")
    if tagmap.get("Duration"):
        dur_str = str(tagmap["Duration"])
        # Try to parse "00:00:05" format
        try:
            parts = dur_str.split(":")
            if len(parts) == 3:
                h, m, s = float(parts[0]), float(parts[1]), float(parts[2])
                data["duration_sec"] = h * 3600 + m * 60 + s
            else:
                data["duration_sec"] = float(dur_str)
        except ValueError:
            data["duration_sec"] = dur_str  # Keep as-is if parsing fails

    # Dimensions
    for w_field, h_field in [("ImageWidth", "ImageHeight"), ("ComposedImageWidth", "ComposedImageHeight")]:
        if tagmap.get(w_field) and tagmap.get(h_field):
            data["width"] = tagmap[w_field]
            data["height"] = tagmap[h_field]
            break

    # Camera model
    for field in ["Model", "CameraModelName"]:
        if tagmap.get(field):
            data["camera_model"] = tagmap[field]
            break

    return data


def main():
    sample_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    out_csv = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("video_metadata_report.csv")

    if not sample_dir.is_dir():
        print(f"Error: {sample_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Video extensions to check
    video_exts = {
        ".mp4", ".mov", ".m4v", ".avi", ".mts", ".m2ts",
        ".3gp", ".mpg", ".mpeg", ".mkv", ".flv", ".wmv", ".webm"
    }

    print(f"Scanning {sample_dir} for video files...")
    files = [p for p in sample_dir.rglob("*") if p.is_file() and p.suffix.lower() in video_exts]
    print(f"Found {len(files)} video files")

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "file",
            "tool",
            "capture_datetime",
            "duration_sec",
            "width",
            "height",
            "codec",
            "camera_model",
            "notes",
        ])

        for i, p in enumerate(sorted(files)):
            rel_path = p.relative_to(sample_dir)
            print(f"[{i+1}/{len(files)}] Processing {rel_path}...", end=" ", flush=True)

            # MediaInfo
            mi_data = media_info_extract(p)
            writer.writerow([
                str(rel_path),
                "mediainfo",
                mi_data.get("capture_datetime"),
                mi_data.get("duration_sec"),
                mi_data.get("width"),
                mi_data.get("height"),
                mi_data.get("codec"),
                mi_data.get("camera_model"),
                mi_data.get("error", ""),
            ])

            # ffprobe
            ff_data = ffprobe_extract(p)
            writer.writerow([
                str(rel_path),
                "ffprobe",
                ff_data.get("capture_datetime"),
                ff_data.get("duration_sec"),
                ff_data.get("width"),
                ff_data.get("height"),
                ff_data.get("codec"),
                ff_data.get("camera_model"),
                ff_data.get("error", ""),
            ])

            # exiftool
            ex_data = exiftool_extract(p)
            writer.writerow([
                str(rel_path),
                "exiftool",
                ex_data.get("capture_datetime"),
                ex_data.get("duration_sec"),
                ex_data.get("width"),
                ex_data.get("height"),
                ex_data.get("codec"),
                ex_data.get("camera_model"),
                ex_data.get("error", ""),
            ])

            print("✓")

    print(f"\nReport written to {out_csv}")


if __name__ == "__main__":
    main()
