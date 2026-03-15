"""
Analytics helpers for BoyoDB.

Provides convenient wrappers for approximate functions, time series, and graph queries.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from .client import Client


@dataclass
class CardinalityEstimate:
    """HyperLogLog cardinality estimate."""
    estimate: int
    relative_error: float  # Typically ~2% for precision 14


@dataclass
class QuantileEstimate:
    """T-Digest quantile estimate."""
    quantile: float
    value: float


@dataclass
class FrequencyEstimate:
    """Count-Min Sketch frequency estimate."""
    item: str
    count: int
    is_overestimate: bool


class ApproximateFunctions:
    """
    Approximate aggregation functions using probabilistic data structures.

    These are much faster than exact counts for large datasets.
    """

    def __init__(self, client: "Client"):
        self._client = client

    def count_distinct(
        self,
        table: str,
        column: str,
        where: Optional[str] = None,
        precision: int = 14,
    ) -> CardinalityEstimate:
        """
        Approximate distinct count using HyperLogLog.

        Args:
            table: Table name
            column: Column to count distinct values
            where: Optional WHERE clause
            precision: HyperLogLog precision (10-18, default 14)

        Returns:
            CardinalityEstimate with count and error rate
        """
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
            SELECT approx_count_distinct({column}, {precision}) as estimate
            FROM {table}
            {where_clause}
        """
        result = self._client.query(sql)
        estimate = result[0]["estimate"] if result else 0

        # Error rate based on precision: 1.04 / sqrt(2^precision)
        error_rate = 1.04 / (2 ** (precision / 2))

        return CardinalityEstimate(
            estimate=int(estimate),
            relative_error=error_rate,
        )

    def percentile(
        self,
        table: str,
        column: str,
        percentiles: Union[float, List[float]],
        where: Optional[str] = None,
        compression: float = 100.0,
    ) -> List[QuantileEstimate]:
        """
        Approximate percentiles using T-Digest.

        Args:
            table: Table name
            column: Numeric column
            percentiles: Single percentile or list (0-100)
            where: Optional WHERE clause
            compression: T-Digest compression (higher = more accurate)

        Returns:
            List of QuantileEstimate
        """
        if isinstance(percentiles, (int, float)):
            percentiles = [percentiles]

        where_clause = f"WHERE {where}" if where else ""
        results = []

        for p in percentiles:
            sql = f"""
                SELECT approx_percentile({column}, {p / 100.0}, {compression}) as value
                FROM {table}
                {where_clause}
            """
            result = self._client.query(sql)
            value = result[0]["value"] if result else None

            results.append(QuantileEstimate(quantile=p, value=value))

        return results

    def median(
        self,
        table: str,
        column: str,
        where: Optional[str] = None,
    ) -> Optional[float]:
        """
        Approximate median (P50).

        Args:
            table: Table name
            column: Numeric column
            where: Optional WHERE clause

        Returns:
            Approximate median value
        """
        estimates = self.percentile(table, column, 50.0, where)
        return estimates[0].value if estimates else None

    def top_k(
        self,
        table: str,
        column: str,
        k: int = 10,
        where: Optional[str] = None,
    ) -> List[FrequencyEstimate]:
        """
        Find top-k most frequent values.

        Args:
            table: Table name
            column: Column to analyze
            k: Number of top items
            where: Optional WHERE clause

        Returns:
            List of FrequencyEstimate
        """
        where_clause = f"WHERE {where}" if where else ""
        sql = f"""
            SELECT {column} as item, COUNT(*) as cnt
            FROM {table}
            {where_clause}
            GROUP BY {column}
            ORDER BY cnt DESC
            LIMIT {k}
        """
        result = self._client.query(sql)

        return [
            FrequencyEstimate(
                item=str(row["item"]),
                count=row["cnt"],
                is_overestimate=False,
            )
            for row in result
        ]


class TimeSeriesAnalytics:
    """Time series analysis functions."""

    def __init__(self, client: "Client"):
        self._client = client

    def aggregate_by_time(
        self,
        table: str,
        time_column: str,
        value_column: str,
        bucket: str,  # '1 minute', '1 hour', '1 day', etc.
        aggregation: str = "avg",
        where: Optional[str] = None,
        fill_gaps: bool = False,
        fill_value: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """
        Aggregate values by time buckets.

        Args:
            table: Table name
            time_column: Timestamp column
            value_column: Value column to aggregate
            bucket: Time bucket size
            aggregation: Aggregation function (avg, sum, min, max, count)
            where: Optional WHERE clause
            fill_gaps: Fill missing time buckets
            fill_value: Value for filled gaps

        Returns:
            List of {bucket, value} dicts
        """
        where_clause = f"WHERE {where}" if where else ""
        agg_func = aggregation.upper()

        sql = f"""
            SELECT
                time_bucket('{bucket}', {time_column}) as bucket,
                {agg_func}({value_column}) as value
            FROM {table}
            {where_clause}
            GROUP BY bucket
            ORDER BY bucket
        """
        return list(self._client.query(sql))

    def moving_average(
        self,
        table: str,
        time_column: str,
        value_column: str,
        window_size: int,
        where: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Calculate moving average.

        Args:
            table: Table name
            time_column: Timestamp column
            value_column: Value column
            window_size: Number of rows in window
            where: Optional WHERE clause

        Returns:
            List of {timestamp, value, moving_avg} dicts
        """
        where_clause = f"WHERE {where}" if where else ""

        sql = f"""
            SELECT
                {time_column} as timestamp,
                {value_column} as value,
                AVG({value_column}) OVER (
                    ORDER BY {time_column}
                    ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW
                ) as moving_avg
            FROM {table}
            {where_clause}
            ORDER BY {time_column}
        """
        return list(self._client.query(sql))

    def detect_anomalies(
        self,
        table: str,
        time_column: str,
        value_column: str,
        threshold_stddev: float = 3.0,
        where: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Detect anomalies using z-score.

        Args:
            table: Table name
            time_column: Timestamp column
            value_column: Value column
            threshold_stddev: Number of standard deviations for anomaly
            where: Optional WHERE clause

        Returns:
            List of anomaly records with z-scores
        """
        where_clause = f"WHERE {where}" if where else ""

        sql = f"""
            WITH stats AS (
                SELECT AVG({value_column}) as mean, STDDEV({value_column}) as stddev
                FROM {table} {where_clause}
            )
            SELECT
                t.{time_column} as timestamp,
                t.{value_column} as value,
                (t.{value_column} - stats.mean) / NULLIF(stats.stddev, 0) as z_score
            FROM {table} t, stats
            {where_clause}
            HAVING ABS(z_score) > {threshold_stddev}
            ORDER BY ABS(z_score) DESC
        """
        return list(self._client.query(sql))


class GraphAnalytics:
    """Graph query helpers."""

    def __init__(self, client: "Client"):
        self._client = client

    def shortest_path(
        self,
        edges_table: str,
        source_column: str,
        target_column: str,
        weight_column: Optional[str],
        start_node: Any,
        end_node: Any,
    ) -> Optional[List[Any]]:
        """
        Find shortest path between two nodes using Dijkstra's algorithm.

        Args:
            edges_table: Table containing edges
            source_column: Column with source node IDs
            target_column: Column with target node IDs
            weight_column: Column with edge weights (optional)
            start_node: Starting node ID
            end_node: Ending node ID

        Returns:
            List of nodes in path, or None if no path exists
        """
        weight = weight_column or "1"

        sql = f"""
            SELECT graph_shortest_path(
                '{edges_table}',
                '{source_column}',
                '{target_column}',
                '{weight}',
                {start_node},
                {end_node}
            ) as path
        """
        result = self._client.query(sql)
        if result and result[0]["path"]:
            return result[0]["path"]
        return None

    def pagerank(
        self,
        edges_table: str,
        source_column: str,
        target_column: str,
        damping: float = 0.85,
        iterations: int = 20,
        top_k: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Calculate PageRank scores.

        Args:
            edges_table: Table containing edges
            source_column: Column with source node IDs
            target_column: Column with target node IDs
            damping: Damping factor (default 0.85)
            iterations: Number of iterations
            top_k: Return top-k nodes

        Returns:
            List of {node, score} dicts
        """
        sql = f"""
            SELECT node, score
            FROM graph_pagerank(
                '{edges_table}',
                '{source_column}',
                '{target_column}',
                {damping},
                {iterations}
            )
            ORDER BY score DESC
            LIMIT {top_k}
        """
        return list(self._client.query(sql))

    def connected_components(
        self,
        edges_table: str,
        source_column: str,
        target_column: str,
    ) -> List[Dict[str, Any]]:
        """
        Find connected components in the graph.

        Returns:
            List of {node, component_id} dicts
        """
        sql = f"""
            SELECT node, component_id
            FROM graph_connected_components(
                '{edges_table}',
                '{source_column}',
                '{target_column}'
            )
            ORDER BY component_id, node
        """
        return list(self._client.query(sql))
