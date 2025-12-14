import pytest
from pathlib import Path
from datetime import datetime, timezone
from photo_organizer.metadata.extract import MetadataExtractor

# Mock MediaInfo class structure
class MockTrack:
    def __init__(self, **kwargs):
        self.track_type = "General"
        for k, v in kwargs.items():
            setattr(self, k, v)

class MockMediaInfo:
    def __init__(self, tracks):
        self.tracks = tracks
    
    @classmethod
    def parse(cls, path):
        # Return specific data based on path for testing
        return cls([MockTrack(
            duration=5000, 
            recorded_date="2023-01-01 12:00:00",
            device_model="TestCam"
        )])

def test_video_metadata_extraction(monkeypatch, tmp_path):
    # Mock the MediaInfo import inside the module
    import photo_organizer.metadata.extract as extract_module
    monkeypatch.setattr(extract_module, "MediaInfo", MockMediaInfo)

    vid = tmp_path / "test.mp4"
    vid.touch()

    extractor = MetadataExtractor()
    dt, dur, cam = extractor.get_video_metadata(vid)

    assert dt == datetime(2023, 1, 1, 12, 0, 0)
    assert dur == 5.0
    assert cam == "TestCam"