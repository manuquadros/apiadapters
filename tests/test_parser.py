from apiadapters.ncbi.parser import is_scanned


def test_is_scanned_true_for_single_supplementary_material() -> None:
    assert is_scanned("<body><supplementary-material/></body>") is True


def test_is_scanned_false_for_multiple_children() -> None:
    assert is_scanned("<body><p>text</p><supplementary-material/></body>") is False


def test_is_scanned_false_for_non_supplementary_child() -> None:
    assert is_scanned("<body><p>real content</p></body>") is False


def test_is_scanned_false_for_empty_body() -> None:
    assert is_scanned("<body/>") is False
