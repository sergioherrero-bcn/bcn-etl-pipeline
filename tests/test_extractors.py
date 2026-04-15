"""Unit tests for src/extract/open_data_bcn.py — extractors only (no DB/network)."""

from __future__ import annotations

import io
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.extract.open_data_bcn import (
    BicingExtractor,
    MeteoExtractor,
    NoiseExtractor,
    GeographyExtractor,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_client(ckan_resources: list[dict] | None = None) -> MagicMock:
    client = MagicMock()
    client.ckan_resources.return_value = ckan_resources or []
    return client


# ── BicingExtractor ───────────────────────────────────────────────────────────

class TestBicingExtractor:
    def _extractor(self, resources=None):
        return BicingExtractor(client=_make_client(resources))

    def test_normalise_info_drops_rows_missing_station_id(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "station_id": [1, None, 3],
            "name":       ["A", "B", "C"],
            "lat":        [41.0, 41.1, 41.2],
            "lon":        [2.1, 2.2, 2.3],
            "capacity":   [20, 15, 25],
        })
        result = ext._normalise_info(raw)
        assert len(result) == 2
        assert set(result["station_id"]) == {1.0, 3.0}

    def test_normalise_status_casts_last_reported(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "station_id":            [1],
            "num_bikes_available":   [5],
            "num_docks_available":   [10],
            "is_installed":          [1],
            "is_renting":            [1],
            "is_returning":          [1],
            "last_reported":         [1_700_000_000],
        })
        result = ext._normalise_status(raw)
        assert pd.api.types.is_datetime64_any_dtype(result["last_reported"])

    def test_find_resource_url_returns_match(self):
        ext = self._extractor()
        resources = [
            {"name": "2024_03_ESTACIONS_INFO.csv", "url": "https://dl/info.csv"},
            {"name": "2024_03_STATUS.csv",          "url": "https://dl/status.csv"},
        ]
        url = ext._find_resource_url(resources, keyword="ESTACIONS_INFO")
        assert url == "https://dl/info.csv"

    def test_find_resource_url_returns_empty_if_no_match(self):
        ext = self._extractor()
        url = ext._find_resource_url([], keyword="ESTACIONS_INFO")
        assert url == ""

    def test_find_archive_url_matches_year_month(self):
        ext = self._extractor()
        resources = [
            {"name": "2024_03_Bicing.7z", "url": "https://dl/2024_03.7z"},
            {"name": "2024_04_Bicing.7z", "url": "https://dl/2024_04.7z"},
        ]
        url = ext._find_archive_url(resources, 2024, 3)
        assert "2024_03" in url


# ── MeteoExtractor ────────────────────────────────────────────────────────────

class TestMeteoExtractor:
    def _extractor(self):
        return MeteoExtractor(client=_make_client())

    def _make_csv_bytes(self) -> bytes:
        df = pd.DataFrame({
            "CODI_ESTACIO": ["X2", "X2"],
            "DATA_LECTURA": ["2024-01-01", "2024-01-01"],
            "ACRONIM":      ["TM", "TX"],
            "VALOR":        [12.5, 18.3],
            "DATA_EXTREM":  [None, "10:30"],
        })
        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        return buf.getvalue()

    def test_normalise_filters_to_known_variables(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "CODI_ESTACIO": ["X2", "X2"],
            "DATA_LECTURA": ["2024-01-01", "2024-01-01"],
            "ACRONIM":      ["TM", "UNKNOWN_VAR"],
            "VALOR":        [12.5, 99.9],
        })
        result = ext._normalise(raw)
        assert len(result) == 1
        assert result.iloc[0]["variable"] == "TM"

    def test_normalise_casts_reading_date(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "CODI_ESTACIO": ["X2"],
            "DATA_LECTURA": ["2024-03-15"],
            "ACRONIM":      ["TM"],
            "VALOR":        [10.0],
        })
        result = ext._normalise(raw)
        from datetime import date
        assert result.iloc[0]["reading_date"] == date(2024, 3, 15)

    def test_normalise_drops_null_values(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "CODI_ESTACIO": ["X2", "X2"],
            "DATA_LECTURA": ["2024-01-01", "2024-01-02"],
            "ACRONIM":      ["TM", "TM"],
            "VALOR":        [None, 5.0],
        })
        result = ext._normalise(raw)
        assert len(result) == 1

    def test_resource_id_for_known_year(self):
        ext = self._extractor()
        rid = ext._resource_id_for_year(2025)
        assert rid == "00904de2-8660-4c41-92e3-66e7c87265be"

    def test_resource_id_falls_back_to_ckan(self):
        client = _make_client(resources=[
            {"id": "abc123", "name": "Meteo_2023_Data.csv"}
        ])
        ext = MeteoExtractor(client=client)
        rid = ext._resource_id_for_year(2023)
        assert rid == "abc123"


# ── NoiseExtractor ────────────────────────────────────────────────────────────

class TestNoiseExtractor:
    def _extractor(self):
        return NoiseExtractor(client=_make_client())

    def test_normalise_applies_db_bounds(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "DATA":   ["01/01/2024", "01/01/2024", "01/01/2024"],
            "HORA":   ["00:00", "00:01", "00:02"],
            "MONITOR": ["M1", "M1", "M1"],
            "LEQ":    [-5.0, 65.0, 145.0],   # -5 and 145 should be filtered
        })
        result = ext._normalise(raw)
        assert len(result) == 1
        assert result.iloc[0]["leq_db"] == 65.0

    def test_normalise_handles_laeq_column_alias(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "DATA":   ["01/01/2024"],
            "LAEQ":  [55.0],
        })
        result = ext._normalise(raw)
        assert "leq_db" in result.columns

    def test_find_archive_url_matches_catalan_month(self):
        ext = self._extractor()
        resources = [
            {"name": "2024_10Oct_XarxaSoroll_EqMonitor_Dades_1Min.zip", "url": "https://dl/oct.zip"}
        ]
        url = ext._find_archive_url(resources, 2024, 10)
        assert url == "https://dl/oct.zip"

    def test_find_archive_url_matches_numeric_month(self):
        ext = self._extractor()
        resources = [
            {"name": "2024_10_Noise.zip", "url": "https://dl/10.zip"}
        ]
        url = ext._find_archive_url(resources, 2024, 10)
        assert url == "https://dl/10.zip"

    def test_find_archive_url_returns_empty_if_missing(self):
        ext = self._extractor()
        url = ext._find_archive_url([], 2024, 10)
        assert url == ""


# ── GeographyExtractor ────────────────────────────────────────────────────────

class TestGeographyExtractor:
    def _extractor(self):
        return GeographyExtractor(client=_make_client())

    def test_normalise_keeps_required_columns(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "CODI_DISTRICTE": [1, 1],
            "NOM_DISTRICTE":  ["Eixample", "Eixample"],
            "CODI_BARRI":     [1, 2],
            "NOM_BARRI":      ["Barrio A", "Barrio B"],
        })
        result = ext._normalise(raw)
        assert "district_id" in result.columns
        assert "neighborhood_id" in result.columns
        assert len(result) == 2

    def test_normalise_drops_null_district(self):
        ext = self._extractor()
        raw = pd.DataFrame({
            "CODI_DISTRICTE": [1, None],
            "NOM_DISTRICTE":  ["Eixample", "Unknown"],
            "CODI_BARRI":     [1, 2],
            "NOM_BARRI":      ["Barrio A", "Barrio B"],
        })
        result = ext._normalise(raw)
        assert len(result) == 1
