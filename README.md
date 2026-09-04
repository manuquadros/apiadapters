# apiadapters

API adapters for services external and internal to D3.

## Installation

This project is managed with [pdm](https://pdm-project.org/). From a clone of
the repository:

```sh
pdm install
```

If you want to depend on it from another project instead, add it with pdm or
pip. Note that one dependency, `xmlparser`, is pulled straight from git
rather than PyPI, so a plain `pip install apiadapters` needs that spelled out
explicitly:

```sh
pip install "apiadapters @ git+https://github.com/<org>/apiadapters.git" \
    "xmlparser @ git+https://github.com/manuquadros/xmlparser.git@master"
```

Requires Python >= 3.12.

## Usage

Each external service has its own adapter. For example, `NCBIAdapter` fetches
abstracts and full-text articles from NCBI's Entrez/PMC APIs:

```python
from apiadapters.ncbi.ncbi import NCBIAdapter

with NCBIAdapter() as api:
    abstracts = api.fetch_ncbi_abstracts("17323951")
    print(abstracts["17323951"])
```

An async equivalent, `AsyncNCBIAdapter`, is also available and is meant to be
used as an async context manager:

```python
import asyncio
from apiadapters.ncbi.ncbi import AsyncNCBIAdapter

async def main() -> None:
    async with AsyncNCBIAdapter() as api:
        is_open = await api.is_pmc_open("365027")
        print(is_open)

asyncio.run(main())
```

Set the `NCBI_API_KEY` environment variable to raise NCBI's rate limits;
adapters work without it.

## Type information

This package ships a `py.typed` marker, so its type annotations are part of
its public API and can be relied on by downstream type checkers.
