"""BigQuery repository helpers used by pipeline services."""

from __future__ import annotations

from typing import Any

from google.cloud import bigquery


class BigQueryRepository:
    """Small wrapper around the BigQuery client for readable service code."""

    def __init__(self, client: bigquery.Client) -> None:
        self.client = client

    def run_query(self, query: str) -> bigquery.table.RowIterator:
        """Execute a SQL query and return row results."""

        return self.client.query(query).result()

    def execute_statement(self, query: str) -> bigquery.job.QueryJob:
        """Execute a SQL statement and wait for completion."""

        job = self.client.query(query)
        job.result()
        return job

    def fetch_one(self, query: str) -> dict[str, Any]:
        """Fetch the first row from a query as a dictionary."""

        rows = list(self.run_query(query))
        if not rows:
            return {}
        return dict(rows[0].items())

    def fetch_all(self, query: str) -> list[dict[str, Any]]:
        """Fetch all rows from a query as dictionaries."""

        return [dict(row.items()) for row in self.run_query(query)]

    def insert_rows_json(self, table_fqn: str, rows: list[dict[str, Any]]) -> list[Any]:
        """Insert JSON rows into a BigQuery table."""

        table = self.client.get_table(table_fqn)
        return self.client.insert_rows_json(table, rows)
