"""
External data source helpers for BoyoDB.

Query S3, URLs, HDFS, and files without importing data.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .client import Client


@dataclass
class S3Config:
    """S3 connection configuration."""
    region: Optional[str] = None
    endpoint: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    session_token: Optional[str] = None
    path_style: bool = False


class ExternalTables:
    """
    Query external data sources without importing.

    Supports S3, HTTP URLs, HDFS, and local files.
    """

    def __init__(self, client: "Client"):
        self._client = client

    def query_s3(
        self,
        path: str,
        sql: str = "SELECT *",
        format: Optional[str] = None,
        config: Optional[S3Config] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query data from S3.

        Args:
            path: S3 path (s3://bucket/path/file.parquet)
            sql: SQL query (use 'data' as table alias)
            format: File format (parquet, csv, json) - auto-detected if not specified
            config: S3 configuration
            limit: Optional row limit

        Returns:
            Query results

        Example:
            results = ext.query_s3(
                "s3://my-bucket/data/events.parquet",
                "SELECT event_type, COUNT(*) FROM data GROUP BY event_type"
            )
        """
        format_arg = f", '{format}'" if format else ""
        limit_clause = f" LIMIT {limit}" if limit else ""

        # Build the query using s3() table function
        full_sql = f"""
            SELECT * FROM (
                {sql.replace('FROM data', f"FROM s3('{path}'{format_arg})")}
            ) AS subq
            {limit_clause}
        """

        # If simple SELECT *, simplify
        if sql.strip().upper() == "SELECT *":
            full_sql = f"SELECT * FROM s3('{path}'{format_arg}){limit_clause}"

        return list(self._client.query(full_sql))

    def query_url(
        self,
        url: str,
        sql: str = "SELECT *",
        format: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query data from HTTP URL.

        Args:
            url: HTTP(S) URL
            sql: SQL query
            format: File format (csv, json, parquet)
            headers: Optional HTTP headers
            limit: Optional row limit

        Returns:
            Query results

        Example:
            results = ext.query_url(
                "https://example.com/data.csv",
                "SELECT * FROM data WHERE value > 100"
            )
        """
        format_arg = f", '{format}'" if format else ""
        limit_clause = f" LIMIT {limit}" if limit else ""

        if sql.strip().upper() == "SELECT *":
            full_sql = f"SELECT * FROM url('{url}'{format_arg}){limit_clause}"
        else:
            full_sql = f"""
                SELECT * FROM (
                    {sql.replace('FROM data', f"FROM url('{url}'{format_arg})")}
                ) AS subq
                {limit_clause}
            """

        return list(self._client.query(full_sql))

    def query_file(
        self,
        path: str,
        sql: str = "SELECT *",
        format: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query data from local file.

        Args:
            path: Local file path
            sql: SQL query
            format: File format
            limit: Optional row limit

        Returns:
            Query results
        """
        format_arg = f", '{format}'" if format else ""
        limit_clause = f" LIMIT {limit}" if limit else ""

        if sql.strip().upper() == "SELECT *":
            full_sql = f"SELECT * FROM file('{path}'{format_arg}){limit_clause}"
        else:
            full_sql = f"""
                SELECT * FROM (
                    {sql.replace('FROM data', f"FROM file('{path}'{format_arg})")}
                ) AS subq
                {limit_clause}
            """

        return list(self._client.query(full_sql))

    def query_hdfs(
        self,
        path: str,
        sql: str = "SELECT *",
        format: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query data from HDFS.

        Args:
            path: HDFS path (hdfs://namenode/path/file)
            sql: SQL query
            format: File format
            limit: Optional row limit

        Returns:
            Query results
        """
        format_arg = f", '{format}'" if format else ""
        limit_clause = f" LIMIT {limit}" if limit else ""

        if sql.strip().upper() == "SELECT *":
            full_sql = f"SELECT * FROM hdfs('{path}'{format_arg}){limit_clause}"
        else:
            full_sql = f"""
                SELECT * FROM (
                    {sql.replace('FROM data', f"FROM hdfs('{path}'{format_arg})")}
                ) AS subq
                {limit_clause}
            """

        return list(self._client.query(full_sql))

    def create_external_table(
        self,
        name: str,
        source_type: str,
        location: str,
        format: str,
        columns: List[Dict[str, str]],
        partition_columns: Optional[List[str]] = None,
    ) -> None:
        """
        Create a named external table.

        Args:
            name: Table name
            source_type: Source type (s3, url, hdfs, file)
            location: Source location
            format: File format
            columns: Column definitions [{"name": "id", "type": "INT64"}, ...]
            partition_columns: Optional partition columns

        Example:
            ext.create_external_table(
                "events_s3",
                "s3",
                "s3://bucket/events/",
                "parquet",
                [
                    {"name": "id", "type": "INT64"},
                    {"name": "event", "type": "STRING"},
                    {"name": "ts", "type": "TIMESTAMP"},
                ],
                partition_columns=["date"]
            )
        """
        col_defs = ", ".join(
            f"{c['name']} {c['type']}" for c in columns
        )
        partition_clause = ""
        if partition_columns:
            partition_clause = f" PARTITION BY ({', '.join(partition_columns)})"

        sql = f"""
            CREATE EXTERNAL TABLE {name} ({col_defs})
            STORED AS {format.upper()}
            LOCATION '{source_type}://{location.lstrip(source_type + "://")}'
            {partition_clause}
        """
        self._client.exec(sql)

    def drop_external_table(self, name: str) -> None:
        """Drop an external table."""
        self._client.exec(f"DROP EXTERNAL TABLE IF EXISTS {name}")


class VectorSearch:
    """
    Vector similarity search helpers.

    For AI/ML embedding search and nearest neighbor queries.
    """

    def __init__(self, client: "Client"):
        self._client = client

    def create_index(
        self,
        table: str,
        column: str,
        index_name: Optional[str] = None,
        index_type: str = "hnsw",
        distance_metric: str = "cosine",
        m: int = 16,
        ef_construction: int = 200,
    ) -> None:
        """
        Create a vector index.

        Args:
            table: Table name
            column: Vector column name
            index_name: Optional index name
            index_type: Index type (hnsw, ivfflat)
            distance_metric: Distance metric (cosine, l2, ip)
            m: HNSW M parameter
            ef_construction: HNSW ef_construction parameter
        """
        name = index_name or f"idx_{table.replace('.', '_')}_{column}_vec"

        sql = f"""
            CREATE INDEX {name} ON {table}
            USING {index_type} ({column} {distance_metric})
            WITH (m = {m}, ef_construction = {ef_construction})
        """
        self._client.exec(sql)

    def search(
        self,
        table: str,
        column: str,
        query_vector: List[float],
        k: int = 10,
        where: Optional[str] = None,
        ef_search: int = 100,
        return_columns: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Find k nearest neighbors.

        Args:
            table: Table name
            column: Vector column
            query_vector: Query vector
            k: Number of results
            where: Optional filter condition
            ef_search: HNSW ef_search parameter
            return_columns: Columns to return (default: all)

        Returns:
            List of results with distance scores
        """
        vec_str = "[" + ",".join(str(v) for v in query_vector) + "]"
        cols = ", ".join(return_columns) if return_columns else "*"
        where_clause = f"WHERE {where}" if where else ""

        sql = f"""
            SELECT {cols}, {column} <=> '{vec_str}'::vector AS distance
            FROM {table}
            {where_clause}
            ORDER BY {column} <=> '{vec_str}'::vector
            LIMIT {k}
        """
        return list(self._client.query(sql))

    def hybrid_search(
        self,
        table: str,
        vector_column: str,
        text_column: str,
        query_vector: List[float],
        query_text: str,
        k: int = 10,
        vector_weight: float = 0.7,
        text_weight: float = 0.3,
    ) -> List[Dict[str, Any]]:
        """
        Hybrid vector + full-text search.

        Args:
            table: Table name
            vector_column: Vector column
            text_column: Text column for FTS
            query_vector: Query vector
            query_text: Query text
            k: Number of results
            vector_weight: Weight for vector similarity
            text_weight: Weight for text relevance

        Returns:
            List of results with combined scores
        """
        vec_str = "[" + ",".join(str(v) for v in query_vector) + "]"
        escaped_text = query_text.replace("'", "''")

        sql = f"""
            SELECT *,
                ({vector_weight} * (1.0 - ({vector_column} <=> '{vec_str}'::vector))) +
                ({text_weight} * ts_rank({text_column}_tsvector, plainto_tsquery('{escaped_text}')))
                AS hybrid_score
            FROM {table}
            WHERE {text_column}_tsvector @@ plainto_tsquery('{escaped_text}')
               OR {vector_column} <=> '{vec_str}'::vector < 0.5
            ORDER BY hybrid_score DESC
            LIMIT {k}
        """
        return list(self._client.query(sql))
