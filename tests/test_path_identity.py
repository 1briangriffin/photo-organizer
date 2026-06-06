from photo_organizer.path_identity import normalize_path_key


def test_normalize_path_key_uses_casefolded_windows_style_keys():
    assert normalize_path_key("C:/Photos/Out/IMG.JPG/") == r"c:\photos\out\img.jpg"
    assert normalize_path_key(r"C:\Photos\Out\IMG.JPG") == r"c:\photos\out\img.jpg"


def test_normalize_path_key_handles_none():
    assert normalize_path_key(None) is None
