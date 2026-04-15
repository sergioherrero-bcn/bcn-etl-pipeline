"""Unit tests for src/utils/db.py"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call


def test_load_dataframe_empty_returns_zero():
    from src.utils.db import load_dataframe

    df = pd.DataFrame()
    result = load_dataframe(df, table="some_table", engine=MagicMock())
    assert result == 0


def test_load_dataframe_returns_row_count():
    from src.utils.db import load_dataframe

    df = pd.DataFrame({"a": [1, 2, 3]})
    mock_engine = MagicMock()

    with patch("src.utils.db.ensure_schema"), \
         patch.object(df.__class__, "to_sql") as mock_to_sql:
        result = load_dataframe(df, table="t", engine=mock_engine)

    assert result == 3


def test_ensure_schema_executes_create():
    from src.utils.db import ensure_schema

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__ = lambda s: mock_conn
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    ensure_schema(mock_engine, "raw")

    mock_conn.execute.assert_called_once()
    sql_text = str(mock_conn.execute.call_args[0][0])
    assert "raw" in sql_text
