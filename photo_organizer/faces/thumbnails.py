"""
Face thumbnail generation and storage.

Extracts aligned face crops from images and saves them as JPEG thumbnails
for visual review in the Streamlit UI.
"""
import logging
from pathlib import Path
from typing import Optional

import numpy as np
from PIL import Image

from . import config


class ThumbnailGenerator:
    """Generates and saves face thumbnail crops."""

    def __init__(self, output_dir: Path,
                 size: tuple[int, int] = config.THUMBNAIL_SIZE):
        self.output_dir = Path(output_dir)
        self.size = size
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_thumbnail(self, img_rgb: np.ndarray,
                       bbox: tuple[int, int, int, int],
                       face_id: int,
                       landmarks: Optional[np.ndarray] = None) -> str:
        """
        Crop face region from image, optionally align, resize, and save.

        Args:
            img_rgb: Full-resolution RGB numpy array (H, W, 3).
            bbox: (x, y, w, h) bounding box of the face.
            face_id: Database ID for the face (used in filename).
            landmarks: Optional (5, 2) facial landmarks for alignment.

        Returns:
            Relative path to saved thumbnail (relative to output_dir parent).
        """
        try:
            x, y, w, h = bbox
            img_h, img_w = img_rgb.shape[:2]

            # Expand bounding box for context (forehead, chin, ears)
            expand = config.THUMBNAIL_BBOX_EXPAND
            pad_x = int(w * expand)
            pad_y = int(h * expand)

            x1 = max(0, x - pad_x)
            y1 = max(0, y - pad_y)
            x2 = min(img_w, x + w + pad_x)
            y2 = min(img_h, y + h + pad_y)

            # Crop face region
            crop = img_rgb[y1:y2, x1:x2]

            if crop.size == 0:
                logging.warning(f"Empty crop for face {face_id}, bbox={bbox}")
                return ""

            # Resize to thumbnail size
            pil_crop = Image.fromarray(crop)
            pil_crop = pil_crop.resize(self.size, Image.LANCZOS)

            # Save path: bucket by face_id // 1000 to avoid filesystem issues
            bucket = f"{face_id // 1000:04d}"
            bucket_dir = self.output_dir / bucket
            bucket_dir.mkdir(parents=True, exist_ok=True)

            filename = f"face_{face_id:06d}.jpg"
            save_path = bucket_dir / filename

            pil_crop.save(save_path, 'JPEG', quality=config.THUMBNAIL_QUALITY)

            # Return path relative to the thumbnail root
            return f"{bucket}/{filename}"

        except Exception as e:
            logging.warning(f"Failed to save thumbnail for face {face_id}: {e}")
            return ""
