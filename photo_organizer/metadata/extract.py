import logging
import subprocess
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Dict, Any

from .. import config

# Optional imports handled gracefully to prevent crashes if libs are missing
try:
    import exifread
except ImportError:
    exifread = None

# Type hint 'Any' prevents Pylance from complaining about "None" having no attribute "parse"
MediaInfo: Any = None
try:
    from pymediainfo import MediaInfo
except ImportError:
    MediaInfo = None


class MetadataExtractor:
    """
    Unified interface for extracting metadata from various file types.
    
    Strategies:
      - Images: Uses 'exifread' (fast, Python-native).
      - Video: Uses 'pymediainfo' (fast wrapper) -> falls back to 'exiftool' (robust).
    """

    def get_image_metadata(self, path: Path) -> Tuple[Optional[datetime], Optional[str], Optional[str]]:
        """
        Extracts metadata from image files (RAW, JPEG, TIFF).
        
        Returns:
            (capture_datetime, camera_model, lens_model)
        """
        if not exifread:
            logging.warning("exifread module not found. Skipping image metadata.")
            return None, None, None

        try:
            with path.open('rb') as f:
                # details=False speeds up processing significantly
                tags = exifread.process_file(f, details=False)

            dt = self._parse_exif_date(tags)
            
            # Extract Camera Model
            camera = None
            if 'Image Model' in tags:
                camera = str(tags['Image Model']).strip()
                
            # Extract Lens Model
            lens = None
            if 'EXIF LensModel' in tags:
                lens = str(tags['EXIF LensModel']).strip()

            return dt, camera, lens
            
        except Exception as e:
            logging.warning(f"ExifRead failed for {path}: {e}")
            return None, None, None

    def get_video_metadata(self, path: Path) -> Tuple[Optional[datetime], Optional[float], Optional[str]]:
        """
        Extracts metadata from video files.
        
        Returns:
            (capture_datetime, duration_sec, camera_model)
        """
        # Strategy 1: Try MediaInfo (Fastest, usually sufficient)
        if MediaInfo is not None:
            try:
                mi_data = self._extract_mediainfo(path)
                # Only return if we actually got useful data. 
                # If both are None, we might as well try ExifTool.
                if mi_data['dt'] or mi_data['duration']:
                    return mi_data['dt'], mi_data['duration'], mi_data['camera']
            except Exception as e:
                logging.debug(f"MediaInfo failed for {path}: {e}")

        # Strategy 2: Try ExifTool (Robust fallback, requires system install)
        # We do NOT use ffmpeg here as requested.
        try:
            et_data = self._extract_exiftool(path)
            if et_data['dt'] or et_data['duration']:
                return et_data['dt'], et_data['duration'], et_data['camera']
        except Exception as e:
            # Only log at debug level to avoid spamming console if tool is missing
            logging.debug(f"ExifTool failed for {path}: {e}")

        return None, None, None

    # --- Internal Extraction Helpers ---

    def _extract_mediainfo(self, path: Path) -> Dict[str, Any]:
        """Parses video using pymediainfo."""
        mi = MediaInfo.parse(str(path))
        data: Dict[str, Any] = {'dt': None, 'duration': None, 'camera': None}
        
        for track in mi.tracks:
            if track.track_type == "General":
                if getattr(track, "duration", None):
                    # MediaInfo duration is in milliseconds
                    data['duration'] = float(track.duration) / 1000.0

                # Priority: Original -> Encoded -> Tagged -> Modified
                # We check multiple fields because different cameras write to different tags.
                date_candidates = [
                    "recorded_date", 
                    "encoded_date", 
                    "tagged_date", 
                    "file_last_modification_date"
                ]
                
                for field in date_candidates:
                    val = getattr(track, field, None)
                    if val:
                        dt = self._parse_flexible_date(val)
                        if dt:
                            data['dt'] = dt
                            break

                # Attempt to find camera model in common fields
                data['camera'] = (
                    getattr(track, "performer", None) or
                    getattr(track, "device_model", None) or
                    getattr(track, "encoded_library", None)
                )
        return data

    def _extract_exiftool(self, path: Path) -> Dict[str, Any]:
        """
        Wraps the 'exiftool' command line utility.
        Must be installed and on the system PATH.
        """
        # -j = JSON output
        # -n = No formatting (returns seconds as float, clean dates)
        cmd = ["exiftool", "-j", "-n", str(path)]
        
        # subprocess.check_output is safer than os.system
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL, text=True)
        data_list = json.loads(out)

        # Explicit type annotation here as well
        data: Dict[str, Any] = {'dt': None, 'duration': None, 'camera': None}
        
        if not data_list:
            return data
        
        tags = data_list[0]
        
        # Date Parsing
        date_fields = ["CreateDate", "CreationDate", "DateTimeOriginal", "MediaCreateDate"]
        for field in date_fields:
            if tags.get(field):
                dt = self._parse_flexible_date(str(tags[field]))
                if dt:
                    data['dt'] = dt
                    break

        # Duration Parsing
        if tags.get("Duration"):
            try:
                # Exiftool with -n returns seconds as float/int
                data['duration'] = float(tags["Duration"])
            except ValueError:
                pass

        # Camera Model
        data['camera'] = (
            tags.get("Model") or 
            tags.get("CameraModelName") or 
            tags.get("Make")
        )

        return data

    def _parse_exif_date(self, tags) -> Optional[datetime]:
        """Helper to parse standard EXIF date strings from exifread."""
        for tag in config.DATE_TAGS:
            if tag in tags:
                try:
                    # EXIF format is usually "YYYY:MM:DD HH:MM:SS"
                    dt_str = str(tags[tag]).replace(':', '-', 2)
                    return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
        return None

    def _parse_flexible_date(self, dt_str: str) -> Optional[datetime]:
        """
        Handles various date formats (ISO, UTC suffixes, Exiftool quirks).
        Returns a naive datetime object.
        """
        if not dt_str: 
            return None
        
        # Clean up common suffixes/prefixes
        clean = dt_str.replace("UTC", "").strip()
        
        # 1. Try ISO format (e.g. 2020-01-01T12:00:00)
        try:
            return datetime.fromisoformat(clean)
        except ValueError:
            pass
            
        # 2. Try Standard EXIF style "YYYY:MM:DD HH:MM:SS"
        try:
            clean_exif = clean.replace(":", "-", 2)
            # Handle potential sub-second precision which strptime hates
            if "." in clean_exif:
                clean_exif = clean_exif.split(".")[0]
            return datetime.strptime(clean_exif, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

        return None