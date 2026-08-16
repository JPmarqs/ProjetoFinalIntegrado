from __future__ import annotations

import csv
import sys
import tempfile
import unittest
from pathlib import Path


DAGS_DIRECTORY = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIRECTORY))

from parquet_utils import convert_csv_to_parquet_file, validate_csv_header
from pipeline_constants import RAW_COLUMNS


class ParquetConversionTest(unittest.TestCase):
    def test_converts_as_strings_and_preserves_expected_nulls(self) -> None:
        import pyarrow.parquet as parquet

        first_row = {column: f"valor-{index}" for index, column in enumerate(RAW_COLUMNS)}
        first_row.update(
            {
                "MUNICIPIO": "Sao Jose",
                "KM": "146,1",
                "SIGLA_DELEGACIA": "DELEGACIA ÁGUIA",
                "QTDE_MORTOS": "0",
            }
        )
        second_row = {column: "NA" for column in RAW_COLUMNS}
        second_row["CD_BAT"] = "000123"
        second_row["MUNICIPIO"] = "N/A"
        second_row["UF_ACIDENTE"] = ""
        source_header = [*RAW_COLUMNS]
        source_header[source_header.index("SIGLA_SUPERINTENDENCIA")] = (
            "Sigla da Superintendência"
        )
        source_header[source_header.index("SIGLA_DELEGACIA")] = "Sigla da Delegacia"
        source_header[source_header.index("SIGLA_UNIDADE_OPERACIONAL")] = (
            "Sigla da Unidade Operacional"
        )

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            csv_path = directory / "fonte.csv"
            parquet_path = directory / "destino.parquet"

            with csv_path.open("w", encoding="latin-1", newline="") as destination:
                writer = csv.writer(
                    destination,
                    delimiter=";",
                    quotechar='"',
                    lineterminator="\r",
                )
                writer.writerow(source_header)
                writer.writerow([first_row[column] for column in RAW_COLUMNS])
                writer.writerow([second_row[column] for column in RAW_COLUMNS])
                destination.write("\r")

            result = convert_csv_to_parquet_file(
                csv_path,
                parquet_path,
                "latin-1",
                RAW_COLUMNS,
            )
            table = parquet.read_table(parquet_path)
            metadata = parquet.read_metadata(parquet_path)

        self.assertEqual(result["parquet_row_count"], 2)
        self.assertEqual(metadata.row_group(0).column(0).compression, "SNAPPY")
        self.assertEqual(table.schema.names, RAW_COLUMNS)
        self.assertTrue(all(str(field.type) == "string" for field in table.schema))
        self.assertEqual(table.column("CD_BAT").to_pylist(), ["valor-0", "000123"])
        self.assertEqual(table.column("KM").to_pylist(), ["146,1", None])
        self.assertEqual(table.column("MUNICIPIO").to_pylist(), ["Sao Jose", None])
        self.assertEqual(table.column("UF_ACIDENTE").to_pylist(), ["valor-2", None])
        self.assertEqual(
            table.column("SIGLA_DELEGACIA").to_pylist()[0],
            "DELEGACIA ÁGUIA",
        )

    def test_rejects_header_outside_raw_contract(self) -> None:
        invalid_columns = [*RAW_COLUMNS]
        invalid_columns[-1] = "COLUNA_DESCONHECIDA"

        with self.assertRaisesRegex(ValueError, "nao corresponde ao contrato RAW"):
            validate_csv_header(invalid_columns, RAW_COLUMNS)


if __name__ == "__main__":
    unittest.main()
