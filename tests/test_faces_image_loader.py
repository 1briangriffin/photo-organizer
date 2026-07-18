"""Tests for image loading utilities."""
import numpy as np
import pytest
from pathlib import Path
from PIL import Image

from photo_organizer.faces.image_loader import (
    load_image_as_bgr, load_image_as_rgb, _maybe_downscale
)


@pytest.fixture
def small_jpeg(tmp_path):
    """Create a small test JPEG."""
    img = Image.new('RGB', (100, 80), color=(255, 0, 0))  # Red image
    path = tmp_path / "test.jpg"
    img.save(path)
    return path


@pytest.fixture
def large_image(tmp_path):
    """Create an image larger than MAX_DETECTION_DIMENSION."""
    img = Image.new('RGB', (4000, 3000), color=(0, 255, 0))
    path = tmp_path / "large.jpg"
    img.save(path)
    return path


@pytest.fixture
def png_image(tmp_path):
    """Create a test PNG with alpha channel."""
    img = Image.new('RGBA', (50, 50), color=(0, 0, 255, 128))
    path = tmp_path / "test.png"
    img.save(path)
    return path


class TestLoadImageAsBGR:
    def test_loads_jpeg(self, small_jpeg):
        result = load_image_as_bgr(small_jpeg, 'jpeg')
        assert result is not None
        assert result.shape == (80, 100, 3)
        assert result.dtype == np.uint8
        # Red in RGB = (255, 0, 0) -> BGR = (0, 0, 255)
        # JPEG compression may shift values slightly
        assert result[0, 0, 2] > 250   # R channel in BGR position
        assert result[0, 0, 0] < 5     # B channel

    def test_loads_png(self, png_image):
        result = load_image_as_bgr(png_image, 'jpeg')  # PNG loaded as 'jpeg' type
        assert result is not None
        assert result.shape == (50, 50, 3)

    def test_returns_none_for_missing_file(self, tmp_path):
        result = load_image_as_bgr(tmp_path / "nonexistent.jpg", 'jpeg')
        assert result is None

    def test_downscales_large_images(self, large_image):
        result = load_image_as_bgr(large_image, 'jpeg')
        assert result is not None
        # Should be downscaled from 4000x3000
        assert max(result.shape[:2]) <= 2048


class TestLoadImageAsRGB:
    def test_loads_jpeg_rgb(self, small_jpeg):
        result = load_image_as_rgb(small_jpeg, 'jpeg')
        assert result is not None
        assert result.shape == (80, 100, 3)
        # Red stays as (255, 0, 0) in RGB
        # JPEG compression may shift values slightly
        assert result[0, 0, 0] > 250   # R
        assert result[0, 0, 1] < 5     # G

    def test_does_not_downscale(self, large_image):
        result = load_image_as_rgb(large_image, 'jpeg')
        assert result is not None
        assert result.shape == (3000, 4000, 3)  # Full resolution


class TestDownscale:
    def test_no_downscale_for_small_image(self):
        img = np.zeros((100, 200, 3), dtype=np.uint8)
        result = _maybe_downscale(img)
        assert result.shape == (100, 200, 3)

    def test_downscale_preserves_aspect_ratio(self):
        img = np.zeros((3000, 4000, 3), dtype=np.uint8)
        result = _maybe_downscale(img)
        h, w = result.shape[:2]
        assert max(h, w) <= 2048
        # Aspect ratio should be preserved (~4:3)
        assert abs(w / h - 4.0 / 3.0) < 0.01
