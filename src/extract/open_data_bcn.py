"""
Extractors for Barcelona Open Data datasets.

Each extractor follows the same contract:
    extract_*() → pd.DataFrame   (fetch + normalise, no DB writes)
    load_*()    → int             (extract + write to raw schema, returns row count)

Datasets covered:
    - Bicing (bike sharing) — monthly CSV archives
    - Meteorology           — daily CSV per year
    - Noise monitoring      — monthly ZIP archives
    - Administrative units  — geographic reference (districts / neighbourhoods)
"""

from __future__ import annotations

import io
import logging
import os
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import py7zr
import yaml

from src.utils.api_client import ApiClient, build_client
from src.utils.db import get_engine, load_dataframe

logger = logging.getLogger(__name__)

# ── Dataset IDs (CKAN package slugs / UUIDs) ─────────────────────────────────
_BICING_PACKAGE_ID  = "6aa3416d-ce1a-494d-861b-7bd07f069600"
_METEO_PACKAGE_ID   = "cf1de5ca-9d1c-424c-9543-8ab23e7f478e"
_NOISE_PACKAGE_ID   = "919d1ce6-6d42-4feb-b2db-3a68ff065b7c"
_GEO_PACKAGE_ID     = "divisions-administratives"   # districts + neighbourhoods

# Catalan month abbreviations used in noise archive filenames
_CAT_MONTHS = {
    1: "Gen", 2: "Feb", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
    7: "Jul", 8: "Ago", 9: "Set", 10: "Oct", 11: "Nov", 12: "Des",
}


# ─────────────────────────────────────────────────────────────────────────────
# Bicing (bike sharing)
# ─────────────────────────────────────────────────────────────────────────────

class BicingExtractor:
    """Extracts Bicing station data from monthly 7z/CSV archives on CKAN."""

    # Expected column mapping from archive CSVs → normalised names
    _INFO_COLS = {
        "station_id": "station_id",
        "name": "station_name",
        "lat": "latitude",
        "lon": "longitude",
        "capacity": "capacity",
        "post_code": "postal_code",
        "cross_street": "cross_street",
    }

    _STATUS_COLS = {
        "station_id": "station_id",
        "num_bikes_available": "num_bikes_available",
        "num_bikes_available_mechanical": "num_bikes_mechanical",
        "num_bikes_available_ebike": "num_bikes_electric",
        "num_docks_available": "num_docks_available",
        "is_installed": "is_installed",
        "is_renting": "is_renting",
        "is_returning": "is_returning",
        "last_reported": "last_reported",
    }

    def __init__(self, client: Optional[ApiClient] = None) -> None:
        self.client = client or build_client()

    # ── public interface ──────────────────────────────────────────────────────

    def extract_station_info(self) -> pd.DataFrame:
        """Return a DataFrame of Bicing station metadata (static reference)."""
        resources = self.client.ckan_resources(_BICING_PACKAGE_ID)
        url = self._find_resource_url(resources, keyword="ESTACIONS_INFO")
        if not url:
            # Fallback: latest monthly archive — extract station info rows
            url = self._latest_archive_url(resources)
            return self._parse_archive_info(url)
        raw = pd.read_csv(url)
        return self._normalise_info(raw)

    def extract_monthly_status(self, year: int, month: int) -> pd.DataFrame:
        """Return status snapshots for a given year/month from the archive."""
        resources = self.client.ckan_resources(_BICING_PACKAGE_ID)
        url = self._find_archive_url(resources, year, month)
        if not url:
            raise ValueError(f"No Bicing archive found for {year}-{month:02d}")
        return self._parse_archive_status(url, year, month)

    def load_station_info(self) -> int:
        df = self.extract_station_info()
        df["_extracted_at"] = datetime.utcnow()
        return load_dataframe(df, table="bicing_station_info", if_exists="replace")

    def load_monthly_status(self, year: int, month: int) -> int:
        df = self.extract_monthly_status(year, month)
        df["_loaded_at"] = datetime.utcnow()
        return load_dataframe(df, table="bicing_station_status")

    # ── private helpers ───────────────────────────────────────────────────────

    def _find_resource_url(self, resources: list[dict], keyword: str) -> str:
        for r in resources:
            if keyword.lower() in r.get("name", "").lower():
                return r["url"]
        return ""

    def _latest_archive_url(self, resources: list[dict]) -> str:
        archives = [
            r for r in resources
            if r.get("format", "").upper() in ("7Z", "ZIP", "CSV")
            and any(kw in r.get("name", "").upper() for kw in ("BICING", "ESTACIONS"))
        ]
        if not archives:
            raise RuntimeError("No Bicing archive resources found in CKAN package")
        # Resources are sorted newest-first by CKAN
        return archives[0]["url"]

    def _find_archive_url(self, resources: list[dict], year: int, month: int) -> str:
        pattern = f"{year}_{month:02d}"
        for r in resources:
            if pattern in r.get("name", ""):
                return r["url"]
        return ""

    def _parse_archive_info(self, url: str) -> pd.DataFrame:
        with tempfile.NamedTemporaryFile(suffix=".7z", delete=False) as tmp:
            self.client.download(url, tmp.name)
            with py7zr.SevenZipFile(tmp.name, mode="r") as archive:
                # Station info files are typically named *ESTACIONS*.csv
                names = [n for n in archive.getnames() if "ESTACION" in n.upper()]
                if not names:
                    names = archive.getnames()[:1]
                extracted = archive.read(names)
                csv_bytes = list(extracted.values())[0].read()
        raw = pd.read_csv(io.BytesIO(csv_bytes))
        return self._normalise_info(raw)

    def _parse_archive_status(self, url: str, year: int, month: int) -> pd.DataFrame:
        with tempfile.NamedTemporaryFile(suffix=".7z", delete=False) as tmp:
            self.client.download(url, tmp.name)
            with py7zr.SevenZipFile(tmp.name, mode="r") as archive:
                names = [n for n in archive.getnames() if n.endswith(".csv")]
                extracted = archive.read(names)

        frames = []
        for name, bio in extracted.items():
            chunk = pd.read_csv(io.BytesIO(bio.read()))
            frames.append(chunk)
        if not frames:
            raise ValueError(f"No CSVs found in archive for {year}-{month:02d}")

        raw = pd.concat(frames, ignore_index=True)
        return self._normalise_status(raw)

    def _normalise_info(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip().str.lower()
        present = {k: v for k, v in self._INFO_COLS.items() if k in df.columns}
        df = df[list(present.keys())].rename(columns=present)
        df["station_id"] = pd.to_numeric(df["station_id"], errors="coerce")
        df["latitude"]   = pd.to_numeric(df["latitude"],   errors="coerce")
        df["longitude"]  = pd.to_numeric(df["longitude"],  errors="coerce")
        df["capacity"]   = pd.to_numeric(df["capacity"],   errors="coerce")
        return df.dropna(subset=["station_id"])

    def _normalise_status(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip().str.lower()
        # Flatten nested types column if present
        if "num_bikes_available_types" in df.columns:
            types = df["num_bikes_available_types"].apply(
                lambda x: x if isinstance(x, dict) else {}
            )
            df["num_bikes_available_mechanical"] = types.apply(lambda x: x.get("mechanical", 0))
            df["num_bikes_available_ebike"]      = types.apply(lambda x: x.get("ebike", 0))

        present = {k: v for k, v in self._STATUS_COLS.items() if k in df.columns}
        df = df[list(present.keys())].rename(columns=present)
        df["station_id"]        = pd.to_numeric(df["station_id"],        errors="coerce")
        df["num_bikes_available"] = pd.to_numeric(df["num_bikes_available"], errors="coerce")
        df["num_docks_available"] = pd.to_numeric(df["num_docks_available"], errors="coerce")
        if "last_reported" in df.columns:
            df["last_reported"] = pd.to_datetime(df["last_reported"], unit="s", utc=True, errors="coerce")
        return df.dropna(subset=["station_id"])


# ─────────────────────────────────────────────────────────────────────────────
# Meteorology (daily statistics, 4 Barcelona stations)
# ─────────────────────────────────────────────────────────────────────────────

class MeteoExtractor:
    """Extracts daily meteorological data from Barcelona's station network.

    The dataset exposes one CSV resource per year. Each row is a
    (station, date, variable, value) tuple — long format.

    Variables extracted (ACRÒNIM field):
        TM  — mean temperature (°C)       TX  — max temperature (°C)
        TN  — min temperature (°C)        HRM — mean relative humidity (%)
        PPT24H — 24h precipitation (mm)   VVM10 — mean wind speed (m/s)
        RS24H  — solar radiation (W/m²)   PN  — station pressure (hPa)
    """

    # CKAN resource IDs by year (extend when new year data becomes available)
    _RESOURCE_BY_YEAR: dict[int, str] = {
        2026: "34726b8b-c1ea-44aa-adcd-1820a7bf9b24",
        2025: "00904de2-8660-4c41-92e3-66e7c87265be",
        2024: "10dbcf20-2e4c-4c94-97e3-68f36ee0056b",
    }

    _BASE_DOWNLOAD = (
        "https://opendata-ajuntament.barcelona.cat/data/dataset/"
        f"{_METEO_PACKAGE_ID}/resource/{{resource_id}}/download"
    )

    _VARIABLES = {"TM", "TX", "TN", "HRM", "PPT24H", "VVM10", "RS24H", "PN"}

    def __init__(self, client: Optional[ApiClient] = None) -> None:
        self.client = client or build_client()

    def extract(self, year: int) -> pd.DataFrame:
        """Return daily meteorological readings for *year* (long format)."""
        resource_id = self._resource_id_for_year(year)
        url = self._BASE_DOWNLOAD.format(resource_id=resource_id)
        logger.info("Fetching meteo CSV for %d from %s", year, url)
        resp = self.client.session.get(url, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(io.BytesIO(resp.content), encoding="utf-8-sig")
        return self._normalise(df)

    def load(self, year: int) -> int:
        df = self.extract(year)
        df["_loaded_at"] = datetime.utcnow()
        return load_dataframe(df, table="meteo_daily")

    def _resource_id_for_year(self, year: int) -> str:
        if year in self._RESOURCE_BY_YEAR:
            return self._RESOURCE_BY_YEAR[year]
        # Fallback: query CKAN and find resource by year pattern in name
        resources = self.client.ckan_resources(_METEO_PACKAGE_ID)
        for r in resources:
            if str(year) in r.get("name", ""):
                return r["id"]
        raise ValueError(f"No meteorological resource found for year {year}")

    def _normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns
            .str.strip()
            .str.upper()
            .str.replace("À", "A").str.replace("Ò", "O")  # strip accents
        )
        col_map = {
            "CODI_ESTACIO": "station_code",
            "DATA_LECTURA": "reading_date",
            "DATA_EXTREM": "extreme_time",
            "ACRONIM":     "variable",
            "VALOR":       "value",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        if "variable" in df.columns:
            df = df[df["variable"].isin(self._VARIABLES)]
        df["reading_date"] = pd.to_datetime(df["reading_date"], errors="coerce").dt.date
        df["value"]        = pd.to_numeric(df["value"], errors="coerce")
        return df.dropna(subset=["station_code", "reading_date", "value"])


# ─────────────────────────────────────────────────────────────────────────────
# Noise monitoring network
# ─────────────────────────────────────────────────────────────────────────────

class NoiseExtractor:
    """Extracts 1-minute noise level measurements from monthly ZIP archives.

    Archive naming convention:
        YYYY_MMNomMes_XarxaSoroll_EqMonitor_Dades_1Min.zip
        e.g. 2025_10Oct_XarxaSoroll_EqMonitor_Dades_1Min.zip

    The ZIP contains one CSV per monitoring equipment unit.
    CSV columns (typical): DATA, HORA, MONITOR_ID, LEQ
    """

    def __init__(self, client: Optional[ApiClient] = None) -> None:
        self.client = client or build_client()

    def extract(self, year: int, month: int) -> pd.DataFrame:
        """Return 1-minute noise readings for the given month."""
        resources = self.client.ckan_resources(_NOISE_PACKAGE_ID)
        url = self._find_archive_url(resources, year, month)
        if not url:
            raise ValueError(f"No noise archive found for {year}-{month:02d}")
        return self._download_and_parse(url)

    def load(self, year: int, month: int) -> int:
        df = self.extract(year, month)
        df["_loaded_at"] = datetime.utcnow()
        return load_dataframe(df, table="noise_readings")

    def _find_archive_url(self, resources: list[dict], year: int, month: int) -> str:
        cat_month = _CAT_MONTHS.get(month, "")
        patterns = [
            f"{year}_{month:02d}",
            f"{year}_{cat_month}",
        ]
        for r in resources:
            name = r.get("name", "")
            if any(p in name for p in patterns):
                return r["url"]
        return ""

    def _download_and_parse(self, url: str) -> pd.DataFrame:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            self.client.download(url, tmp.name)
            frames = []
            with zipfile.ZipFile(tmp.name) as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                for name in csv_names:
                    with zf.open(name) as fh:
                        try:
                            chunk = pd.read_csv(fh, sep=";", encoding="utf-8-sig", on_bad_lines="skip")
                            chunk["_source_file"] = name
                            frames.append(chunk)
                        except Exception as exc:
                            logger.warning("Skipping %s: %s", name, exc)

        if not frames:
            raise ValueError("No valid CSVs in noise archive")
        df = pd.concat(frames, ignore_index=True)
        return self._normalise(df)

    def _normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip().str.upper()
        col_map = {
            "DATA":       "reading_date",
            "HORA":       "reading_time",
            "MONITOR":    "monitor_id",
            "ID_MONITOR": "monitor_id",
            "LEQ":        "leq_db",
            "LAEQ":       "leq_db",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        required = {"reading_date", "leq_db"}
        if not required.issubset(df.columns):
            logger.error("Noise CSV missing expected columns. Got: %s", list(df.columns))
            return pd.DataFrame(columns=["reading_date", "monitor_id", "leq_db"])
        df["reading_date"] = pd.to_datetime(df["reading_date"], dayfirst=True, errors="coerce").dt.date
        df["leq_db"]       = pd.to_numeric(df["leq_db"], errors="coerce")
        # Hard physical bounds: noise levels must be 0–140 dB
        df = df[(df["leq_db"] >= 0) & (df["leq_db"] <= 140)]
        return df.dropna(subset=["reading_date", "leq_db"])


# ─────────────────────────────────────────────────────────────────────────────
# Administrative units (geographic reference)
# ─────────────────────────────────────────────────────────────────────────────

class GeographyExtractor:
    """Extracts district and neighbourhood reference data.

    This is a slowly-changing dimension loaded once (or on schema refresh).
    Barcelona has 10 districts and 73 neighbourhoods.
    """

    # Direct CSV endpoint for neighbourhood / district units
    _NEIGHBOURHOODS_URL = (
        "https://opendata-ajuntament.barcelona.cat/data/api/action/"
        "datastore_search?resource_id=e5b4f1a1-f3a8-4a95-a8a6-58c388e70c48&limit=100"
    )
    # Fallback: CKAN package with shapefiles and CSVs
    _GEO_PACKAGE_ID = "divisions-administratives"

    def __init__(self, client: Optional[ApiClient] = None) -> None:
        self.client = client or build_client()

    def extract(self) -> pd.DataFrame:
        """Return a DataFrame with district + neighbourhood hierarchy."""
        try:
            return self._fetch_from_datastore()
        except Exception as exc:
            logger.warning("Datastore fetch failed (%s), falling back to CKAN package", exc)
            return self._fetch_from_package()

    def load(self) -> int:
        df = self.extract()
        df["_extracted_at"] = datetime.utcnow()
        return load_dataframe(df, table="administrative_units", if_exists="replace")

    def _fetch_from_datastore(self) -> pd.DataFrame:
        data = self.client.get_url(self._NEIGHBOURHOODS_URL)
        records = data.get("result", {}).get("records", [])
        df = pd.DataFrame(records)
        return self._normalise(df)

    def _fetch_from_package(self) -> pd.DataFrame:
        resources = self.client.ckan_resources(self._GEO_PACKAGE_ID)
        csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
        if not csv_resources:
            raise RuntimeError("No CSV resource found in administrative units package")
        url = csv_resources[0]["url"]
        resp = self.client.session.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.BytesIO(resp.content), encoding="utf-8-sig")
        return self._normalise(df)

    def _normalise(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = df.columns.str.strip().str.upper()
        col_map = {
            "CODI_DISTRICTE": "district_id",
            "NOM_DISTRICTE":  "district_name",
            "CODI_BARRI":     "neighborhood_id",
            "NOM_BARRI":      "neighborhood_name",
            "LATITUD":        "latitude",
            "LONGITUD":       "longitude",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        for col in ("district_id", "neighborhood_id"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in ("latitude", "longitude"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        keep = [c for c in col_map.values() if c in df.columns]
        return df[keep].drop_duplicates().dropna(subset=["district_id"])
