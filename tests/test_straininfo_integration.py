import pytest
import pytest_asyncio
from apiadapters.straininfo.straininfo import AsyncStrainInfoAdapter, StrainInfoAdapter
from d3types import Strain

KNOWN_DESIGNATION = "DSM 498"


@pytest.mark.integration
def test_get_strain_ids_returns_ints_for_known_designation() -> None:
    with StrainInfoAdapter() as adapter:
        ids = adapter.get_strain_ids(KNOWN_DESIGNATION)
    assert isinstance(ids, list)
    assert len(ids) > 0
    assert all(isinstance(i, int) for i in ids)


@pytest.mark.integration
def test_get_strain_data_returns_strain_models() -> None:
    with StrainInfoAdapter() as adapter:
        ids = adapter.get_strain_ids(KNOWN_DESIGNATION)
        strains = adapter.get_strain_data(ids)
    assert len(strains) > 0
    assert all(isinstance(s, Strain) for s in strains)


@pytest.mark.integration
def test_strain_model_has_designations() -> None:
    with StrainInfoAdapter() as adapter:
        ids = adapter.get_strain_ids(KNOWN_DESIGNATION)
        strains = adapter.get_strain_data(ids)
    assert any(len(s.designations) > 0 for s in strains)


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
async def test_async_get_strain_data_returns_strain_models() -> None:
    async with AsyncStrainInfoAdapter() as adapter:
        ids = await adapter.get_strain_ids(KNOWN_DESIGNATION)
        strains = await adapter.get_strain_data(ids)
    assert len(strains) > 0
    assert all(isinstance(s, Strain) for s in strains)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_retrieve_strain_models_roundtrip() -> None:
    """Full roundtrip: seed a dict with a known strain, retrieve enriched models."""
    seed_strain = Strain(
        designations=frozenset({KNOWN_DESIGNATION}), cultures=frozenset()
    )
    strains = {0: seed_strain}
    async with AsyncStrainInfoAdapter() as adapter:
        result = await adapter.retrieve_strain_models(strains)
    enriched = result[0]
    assert enriched is not seed_strain
    assert isinstance(enriched, Strain)
