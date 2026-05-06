import pytest
from apiadapters.ncbi.taxonomy import NCBIDump


def test_readline_strips_ncbi_line_terminator(tmp_path) -> None:
    f = tmp_path / "names.dmp"
    f.write_bytes(b"1\t|Bacteria\t|\n")
    with NCBIDump(str(f)) as dump:
        assert dump.readline() == "1\t|Bacteria"


def test_readline_on_empty_file_returns_empty_string(tmp_path) -> None:
    f = tmp_path / "names.dmp"
    f.write_bytes(b"")
    with NCBIDump(str(f)) as dump:
        assert dump.readline() == ""


def test_read_replaces_field_separators(tmp_path) -> None:
    f = tmp_path / "names.dmp"
    f.write_bytes(b"1\t|Bacteria\t|\n2\t|Archaea\t|\n")
    with NCBIDump(str(f)) as dump:
        assert dump.read() == "1\t|Bacteria\n2\t|Archaea\n"


def test_context_manager_closes_file(tmp_path) -> None:
    f = tmp_path / "names.dmp"
    f.write_bytes(b"1\t|Bacteria\t|\n")
    with NCBIDump(str(f)) as dump:
        inner = dump._file
    assert inner.closed
