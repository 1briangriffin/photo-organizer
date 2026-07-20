"""
Unified image loader for face detection.

Loads JPEG, PNG, TIFF, and RAW files as BGR numpy arrays
suitable for insightface processing.
"""
import logging
from pathlib import Path
from typing import Optional

import numpy as np
from PIL import Image, ImageOps

from . import config

try:
    import rawpy
except ImportError:
    rawpy = None


def load_image_as_bgr(path: Path, file_type: str) -> Optional[np.ndarray]:
    """
    Load any supported image file as a BGR numpy array for insightface.

    Args:
        path: Path to the image file.
        file_type: File type from the catalog ('jpeg', 'raw', 'tiff').

    Returns:
        numpy array of shape (H, W, 3) dtype uint8 in BGR order,
        or None on failure. Large images are downscaled to
        MAX_DETECTION_DIMENSION on the long edge.
    """
    try:
        if file_type == 'raw':
            img_rgb = _load_raw(path)
        else:
            img_rgb = _load_pil(path)

        if img_rgb is None:
            return None

        # Downscale if needed for detection performance
        img_rgb = _maybe_downscale(img_rgb)

        # Convert RGB -> BGR for insightface
        return img_rgb[:, :, ::-1].copy()

    except Exception as e:
        logging.warning(f"Failed to load image {path}: {e}")
        return None


def load_image_as_rgb(path: Path, file_type: str) -> Optional[np.ndarray]:
    """
    Load an image as RGB numpy array (for thumbnail cropping from full-res).

    Does NOT downscale - returns the original resolution.
    """
    try:
        if file_type == 'raw':
            return _load_raw(path)
        else:
            return _load_pil(path)
    except Exception as e:
        logging.warning(f"Failed to load image {path}: {e}")
        return None


def _load_pil(path: Path) -> Optional[np.ndarray]:
    """Load JPEG/PNG/TIFF via Pillow, returning RGB uint8 array in the
    UPRIGHT orientation."""
    img = Image.open(path)

    # Cameras and phones often store rotated pixels plus an EXIF
    # orientation tag. Detection must see the upright image — SCRFD finds
    # sideways faces poorly — and every consumer of detection bboxes must
    # live in the same (upright) coordinate space. RAW files don't pass
    # through here: rawpy/LibRaw applies the orientation flag itself.
    img = ImageOps.exif_transpose(img)

    # Handle 16-bit TIFFs and other modes
    if img.mode == 'I;16':
        # Convert 16-bit to 8-bit
        arr = np.array(img, dtype=np.uint16)
        arr = (arr >> 8).astype(np.uint8)
        # Convert single channel to RGB
        if arr.ndim == 2:
            arr = np.stack([arr, arr, arr], axis=-1)
        return arr

    img = img.convert('RGB')
    return np.array(img, dtype=np.uint8)


def read_exif_orientation(path: Path) -> Optional[int]:
    """The EXIF orientation tag (1-8), or None when absent/unreadable.
    Cheap: only the image header is parsed, no pixel decode."""
    try:
        with Image.open(path) as img:
            value = img.getexif().get(0x0112)
        return int(value) if value else None
    except Exception:
        return None


def _load_raw(path: Path) -> Optional[np.ndarray]:
    """Load RAW file via rawpy, returning RGB uint8 array."""
    if rawpy is None:
        logging.warning(
            f"rawpy not installed, cannot process RAW file: {path}. "
            f"Install with: uv add rawpy"
        )
        return None

    with rawpy.imread(str(path)) as raw:
        rgb = raw.postprocess(
            use_camera_wb=True,
            half_size=False,
            no_auto_bright=False,
            output_bps=8,
        )
    return rgb


def _maybe_downscale(img: np.ndarray) -> np.ndarray:
    """
    Downscale image if either dimension exceeds MAX_DETECTION_DIMENSION.

    Maintains aspect ratio. Uses Pillow for high-quality downscaling.
    """
    h, w = img.shape[:2]
    max_dim = config.MAX_DETECTION_DIMENSION

    if max(h, w) <= max_dim:
        return img

    scale = max_dim / max(h, w)
    new_w = int(w * scale)
    new_h = int(h * scale)

    pil_img = Image.fromarray(img)
    pil_img = pil_img.resize((new_w, new_h), Image.LANCZOS)
    return np.array(pil_img)
