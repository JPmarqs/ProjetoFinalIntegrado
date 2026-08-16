"""Conversao validada e incremental do CSV da PRF para Parquet."""

from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path
from typing import Any, Sequence


CSV_HEADER_ALIASES = {
    "SIGLA_DA_SUPERINTENDENCIA": "SIGLA_SUPERINTENDENCIA",
    "SIGLA_DA_DELEGACIA": "SIGLA_DELEGACIA",
    "SIGLA_DA_UNIDADE_OPERACIONAL": "SIGLA_UNIDADE_OPERACIONAL",
}


def normalize_csv_column_name(column: str) -> str:
    """Normaliza caixa, acentos e separadores dos cabecalhos da fonte."""
    without_accents = "".join(
        character
        for character in unicodedata.normalize("NFKD", column)
        if not unicodedata.combining(character)
    )
    normalized = re.sub(r"[^A-Z0-9]+", "_", without_accents.upper()).strip("_")
    return CSV_HEADER_ALIASES.get(normalized, normalized)


def read_csv_header(csv_path: Path, encoding: str) -> list[str]:
    """Le o cabecalho com as mesmas regras de delimitacao usadas na conversao."""
    with csv_path.open("r", encoding=encoding, newline="") as source:
        header = next(
            csv.reader(source, delimiter=";", quotechar='"'),
            None,
        )

    if header is None:
        raise ValueError(f"O CSV nao possui cabecalho: {csv_path}")

    return [column.lstrip("\ufeff").strip() for column in header]


def validate_csv_header(
    actual_columns: Sequence[str],
    expected_columns: Sequence[str],
) -> None:
    """Exige o contrato completo antes que dados sejam publicados como Parquet."""
    normalized_actual = [normalize_csv_column_name(column) for column in actual_columns]
    normalized_expected = [
        normalize_csv_column_name(column) for column in expected_columns
    ]

    if len(set(normalized_actual)) != len(normalized_actual):
        raise ValueError(f"O CSV possui colunas duplicadas: {list(actual_columns)}")

    if normalized_actual != normalized_expected:
        raise ValueError(
            "O cabecalho do CSV nao corresponde ao contrato RAW. "
            f"Esperado: {list(expected_columns)}. "
            f"Recebido: {list(actual_columns)}. "
            f"Recebido apos normalizacao: {normalized_actual}."
        )


def convert_csv_to_parquet_file(
    csv_path: Path,
    parquet_path: Path,
    encoding: str,
    expected_columns: Sequence[str],
) -> dict[str, Any]:
    """Converte o CSV em lotes, preservando todas as colunas como texto."""
    import pyarrow as pa
    import pyarrow.csv as arrow_csv
    import pyarrow.parquet as parquet

    actual_columns = read_csv_header(csv_path, encoding)
    validate_csv_header(actual_columns, expected_columns)

    canonical_columns = list(expected_columns)
    column_types = {column: pa.string() for column in canonical_columns}
    temporary_path = parquet_path.with_suffix(f"{parquet_path.suffix}.tmp")

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path.unlink(missing_ok=True)

    reader = arrow_csv.open_csv(
        csv_path,
        read_options=arrow_csv.ReadOptions(
            column_names=canonical_columns,
            skip_rows=1,
            encoding=encoding,
            block_size=8 * 1024 * 1024,
        ),
        parse_options=arrow_csv.ParseOptions(
            delimiter=";",
            quote_char='"',
            double_quote=True,
            newlines_in_values=False,
            ignore_empty_lines=True,
        ),
        convert_options=arrow_csv.ConvertOptions(
            column_types=column_types,
            strings_can_be_null=True,
            quoted_strings_can_be_null=True,
            null_values=["", "NA", "N/A"],
        ),
    )

    row_count = 0

    try:
        with parquet.ParquetWriter(
            temporary_path,
            reader.schema,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        ) as writer:
            for batch in reader:
                writer.write_batch(batch)
                row_count += batch.num_rows

        if row_count <= 0:
            raise ValueError("A conversao nao produziu nenhuma linha.")

        parquet_columns = parquet.read_schema(temporary_path).names
        parquet_rows = parquet.read_metadata(temporary_path).num_rows

        if parquet_columns != canonical_columns:
            raise ValueError(
                "O schema Parquet gerado nao corresponde ao contrato RAW. "
                f"Esperado: {canonical_columns}. Recebido: {parquet_columns}."
            )

        if parquet_rows != row_count:
            raise ValueError(
                "A quantidade de linhas do rodape Parquet diverge da conversao: "
                f"rodape={parquet_rows}, conversao={row_count}."
            )

        parquet_path.unlink(missing_ok=True)
        temporary_path.replace(parquet_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    return {
        "parquet_path": str(parquet_path),
        "parquet_filename": parquet_path.name,
        "parquet_size_bytes": parquet_path.stat().st_size,
        "parquet_row_count": row_count,
        "parquet_columns": canonical_columns,
        "parquet_compression": "snappy",
    }
