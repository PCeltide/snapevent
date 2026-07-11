from conftest import MIXED, PURE_WS

import kdp_data


def test_public_errors_exported():
    assert issubclass(kdp_data.IncompleteData, kdp_data.KdpDataError)
    assert issubclass(kdp_data.UnsupportedSchema, kdp_data.KdpDataError)
    assert issubclass(kdp_data.MissingTable, kdp_data.KdpDataError)


def test_fixtures_present():
    # The package's whole test suite leans on the committed Rust fixtures;
    # fail loudly here if the relative path ever breaks.
    assert (PURE_WS / "manifest.json").exists()
    assert (MIXED / "manifest.json").exists()
