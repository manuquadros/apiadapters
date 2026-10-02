import pytest
import pytest_asyncio
from apiadapters.straininfo import AsyncStrainInfoAdapter, StrainInfoAdapter

KNOWN_DESIGNATION = "DSM 498"


@pytest.mark.integration
def test_get_strain_ids_returns_ints_for_known_designation() -> None:
    with StrainInfoAdapter() as adapter:
        ids = adapter.get_strain_ids(KNOWN_DESIGNATION)
    assert isinstance(ids, list)
    assert len(ids) > 0
    assert all(isinstance(i, int) for i in ids)


@pytest.mark.integration
def test_get_strain_data_returns_strain_records() -> None:
    with StrainInfoAdapter() as adapter:
        ids = adapter.get_strain_ids(KNOWN_DESIGNATION)
        strains = adapter.get_strain_data(ids)
    assert len(strains) > 0
    assert all(isinstance(s, dict) and isinstance(s["id"], int) for s in strains)


@pytest.mark.integration
def test_strain_record_has_designations() -> None:
    with StrainInfoAdapter() as adapter:
        ids = adapter.get_strain_ids(KNOWN_DESIGNATION)
        strains = adapter.get_strain_data(ids)
    assert any(s["relation"].get("designation") for s in strains)


@pytest.mark.integration
def test_get_strain_ids_empty_for_nonexistent_designation() -> None:
    with StrainInfoAdapter() as adapter:
        ids = adapter.get_strain_ids("ZZZNONSENSE99999")
    assert ids == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_get_strain_ids_returns_ints_for_known_designation() -> None:
    async with AsyncStrainInfoAdapter() as adapter:
        ids = await adapter.get_strain_ids(KNOWN_DESIGNATION)
    assert isinstance(ids, list)
    assert len(ids) > 0
    assert all(isinstance(i, int) for i in ids)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_get_strain_data_returns_strain_records() -> None:
    async with AsyncStrainInfoAdapter() as adapter:
        ids = await adapter.get_strain_ids(KNOWN_DESIGNATION)
        strains = await adapter.get_strain_data(ids)
    assert len(strains) > 0
    assert all(isinstance(s, dict) and isinstance(s["id"], int) for s in strains)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_retrieve_strain_models_roundtrip() -> None:
    """Full roundtrip: look up a known designation, retrieve its record."""
    designations = {0: {KNOWN_DESIGNATION}}
    async with AsyncStrainInfoAdapter() as adapter:
        result = await adapter.retrieve_strain_models(designations)
    assert designations == {0: {KNOWN_DESIGNATION}}
    assert isinstance(result[0]["id"], int)
