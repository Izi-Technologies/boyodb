package boyodb

import (
	"fmt"
	"math"
	"strings"
)

// CardinalityEstimate represents a HyperLogLog cardinality estimate.
type CardinalityEstimate struct {
	Estimate      int64
	RelativeError float64
}

// QuantileEstimate represents a T-Digest quantile estimate.
type QuantileEstimate struct {
	Quantile float64
	Value    float64
}

// FrequencyEstimate represents a frequency estimate from Count-Min Sketch.
type FrequencyEstimate struct {
	Item          string
	Count         int64
	IsOverestimate bool
}

// ApproximateFunctions provides approximate analytics using probabilistic data structures.
type ApproximateFunctions struct {
	client *Client
}

// Approximate returns an ApproximateFunctions instance.
func (c *Client) Approximate() *ApproximateFunctions {
	return &ApproximateFunctions{client: c}
}

// CountDistinct estimates distinct count using HyperLogLog.
func (af *ApproximateFunctions) CountDistinct(table, column string, where string, precision int) (*CardinalityEstimate, error) {
	if precision == 0 {
		precision = 14
	}

	whereClause := ""
	if where != "" {
		whereClause = fmt.Sprintf("WHERE %s", where)
	}

	sql := fmt.Sprintf(`
		SELECT approx_count_distinct(%s, %d) as estimate
		FROM %s
		%s
	`, column, precision, table, whereClause)

	result, err := af.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	estimate := int64(0)
	if result.Next() {
		if err := result.Scan(&estimate); err != nil {
			return nil, err
		}
	}

	// Error rate based on precision: 1.04 / sqrt(2^precision)
	errorRate := 1.04 / math.Sqrt(math.Pow(2, float64(precision)))

	return &CardinalityEstimate{
		Estimate:      estimate,
		RelativeError: errorRate,
	}, nil
}

// Percentile estimates percentiles using T-Digest.
func (af *ApproximateFunctions) Percentile(table, column string, percentiles []float64, where string, compression float64) ([]QuantileEstimate, error) {
	if compression == 0 {
		compression = 100.0
	}

	whereClause := ""
	if where != "" {
		whereClause = fmt.Sprintf("WHERE %s", where)
	}

	results := make([]QuantileEstimate, 0, len(percentiles))

	for _, p := range percentiles {
		sql := fmt.Sprintf(`
			SELECT approx_percentile(%s, %f, %f) as value
			FROM %s
			%s
		`, column, p/100.0, compression, table, whereClause)

		result, err := af.client.Query(sql)
		if err != nil {
			return nil, err
		}

		var value float64
		if result.Next() {
			if err := result.Scan(&value); err != nil {
				result.Close()
				return nil, err
			}
		}
		result.Close()

		results = append(results, QuantileEstimate{
			Quantile: p,
			Value:    value,
		})
	}

	return results, nil
}

// Median estimates the median (P50).
func (af *ApproximateFunctions) Median(table, column, where string) (float64, error) {
	estimates, err := af.Percentile(table, column, []float64{50.0}, where, 0)
	if err != nil {
		return 0, err
	}
	if len(estimates) > 0 {
		return estimates[0].Value, nil
	}
	return 0, nil
}

// TopK finds the top-k most frequent values.
func (af *ApproximateFunctions) TopK(table, column string, k int, where string) ([]FrequencyEstimate, error) {
	if k == 0 {
		k = 10
	}

	whereClause := ""
	if where != "" {
		whereClause = fmt.Sprintf("WHERE %s", where)
	}

	sql := fmt.Sprintf(`
		SELECT %s as item, COUNT(*) as cnt
		FROM %s
		%s
		GROUP BY %s
		ORDER BY cnt DESC
		LIMIT %d
	`, column, table, whereClause, column, k)

	result, err := af.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	results := make([]FrequencyEstimate, 0)
	for result.Next() {
		var item string
		var count int64
		if err := result.Scan(&item, &count); err != nil {
			return nil, err
		}
		results = append(results, FrequencyEstimate{
			Item:          item,
			Count:         count,
			IsOverestimate: false,
		})
	}

	return results, nil
}

// TimeSeriesAnalytics provides time series analysis functions.
type TimeSeriesAnalytics struct {
	client *Client
}

// TimeSeries returns a TimeSeriesAnalytics instance.
func (c *Client) TimeSeries() *TimeSeriesAnalytics {
	return &TimeSeriesAnalytics{client: c}
}

// TimeBucket represents a time bucket aggregation result.
type TimeBucket struct {
	Bucket string
	Value  float64
}

// AggregateByTime aggregates values by time buckets.
func (ts *TimeSeriesAnalytics) AggregateByTime(table, timeColumn, valueColumn, bucket, aggregation, where string) ([]TimeBucket, error) {
	if aggregation == "" {
		aggregation = "avg"
	}

	whereClause := ""
	if where != "" {
		whereClause = fmt.Sprintf("WHERE %s", where)
	}

	sql := fmt.Sprintf(`
		SELECT
			time_bucket('%s', %s) as bucket,
			%s(%s) as value
		FROM %s
		%s
		GROUP BY bucket
		ORDER BY bucket
	`, bucket, timeColumn, strings.ToUpper(aggregation), valueColumn, table, whereClause)

	result, err := ts.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	results := make([]TimeBucket, 0)
	for result.Next() {
		var bucket string
		var value float64
		if err := result.Scan(&bucket, &value); err != nil {
			return nil, err
		}
		results = append(results, TimeBucket{
			Bucket: bucket,
			Value:  value,
		})
	}

	return results, nil
}

// MovingAverageResult represents a moving average calculation result.
type MovingAverageResult struct {
	Timestamp  string
	Value      float64
	MovingAvg  float64
}

// MovingAverage calculates moving average.
func (ts *TimeSeriesAnalytics) MovingAverage(table, timeColumn, valueColumn string, windowSize int, where string) ([]MovingAverageResult, error) {
	whereClause := ""
	if where != "" {
		whereClause = fmt.Sprintf("WHERE %s", where)
	}

	sql := fmt.Sprintf(`
		SELECT
			%s as timestamp,
			%s as value,
			AVG(%s) OVER (
				ORDER BY %s
				ROWS BETWEEN %d PRECEDING AND CURRENT ROW
			) as moving_avg
		FROM %s
		%s
		ORDER BY %s
	`, timeColumn, valueColumn, valueColumn, timeColumn, windowSize-1, table, whereClause, timeColumn)

	result, err := ts.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	results := make([]MovingAverageResult, 0)
	for result.Next() {
		var r MovingAverageResult
		if err := result.Scan(&r.Timestamp, &r.Value, &r.MovingAvg); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}

// AnomalyResult represents an anomaly detection result.
type AnomalyResult struct {
	Timestamp string
	Value     float64
	ZScore    float64
}

// DetectAnomalies detects anomalies using z-score.
func (ts *TimeSeriesAnalytics) DetectAnomalies(table, timeColumn, valueColumn string, thresholdStddev float64, where string) ([]AnomalyResult, error) {
	if thresholdStddev == 0 {
		thresholdStddev = 3.0
	}

	whereClause := ""
	if where != "" {
		whereClause = fmt.Sprintf("WHERE %s", where)
	}

	sql := fmt.Sprintf(`
		WITH stats AS (
			SELECT AVG(%s) as mean, STDDEV(%s) as stddev
			FROM %s %s
		)
		SELECT
			t.%s as timestamp,
			t.%s as value,
			(t.%s - stats.mean) / NULLIF(stats.stddev, 0) as z_score
		FROM %s t, stats
		%s
		HAVING ABS(z_score) > %f
		ORDER BY ABS(z_score) DESC
	`, valueColumn, valueColumn, table, whereClause, timeColumn, valueColumn, valueColumn, table, whereClause, thresholdStddev)

	result, err := ts.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	results := make([]AnomalyResult, 0)
	for result.Next() {
		var r AnomalyResult
		if err := result.Scan(&r.Timestamp, &r.Value, &r.ZScore); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}

// GraphAnalytics provides graph analysis functions.
type GraphAnalytics struct {
	client *Client
}

// Graph returns a GraphAnalytics instance.
func (c *Client) Graph() *GraphAnalytics {
	return &GraphAnalytics{client: c}
}

// ShortestPath finds the shortest path between two nodes.
func (ga *GraphAnalytics) ShortestPath(edgesTable, sourceColumn, targetColumn, weightColumn string, startNode, endNode interface{}) ([]interface{}, error) {
	weight := weightColumn
	if weight == "" {
		weight = "1"
	}

	sql := fmt.Sprintf(`
		SELECT graph_shortest_path(
			'%s',
			'%s',
			'%s',
			'%s',
			%v,
			%v
		) as path
	`, edgesTable, sourceColumn, targetColumn, weight, startNode, endNode)

	result, err := ga.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	if result.Next() {
		var path []interface{}
		if err := result.Scan(&path); err != nil {
			return nil, err
		}
		return path, nil
	}

	return nil, nil
}

// PageRankResult represents a PageRank result.
type PageRankResult struct {
	Node  interface{}
	Score float64
}

// PageRank calculates PageRank scores.
func (ga *GraphAnalytics) PageRank(edgesTable, sourceColumn, targetColumn string, damping float64, iterations, topK int) ([]PageRankResult, error) {
	if damping == 0 {
		damping = 0.85
	}
	if iterations == 0 {
		iterations = 20
	}
	if topK == 0 {
		topK = 10
	}

	sql := fmt.Sprintf(`
		SELECT node, score
		FROM graph_pagerank(
			'%s',
			'%s',
			'%s',
			%f,
			%d
		)
		ORDER BY score DESC
		LIMIT %d
	`, edgesTable, sourceColumn, targetColumn, damping, iterations, topK)

	result, err := ga.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	results := make([]PageRankResult, 0)
	for result.Next() {
		var r PageRankResult
		if err := result.Scan(&r.Node, &r.Score); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}

// ConnectedComponentResult represents a connected component result.
type ConnectedComponentResult struct {
	Node        interface{}
	ComponentID int64
}

// ConnectedComponents finds connected components in the graph.
func (ga *GraphAnalytics) ConnectedComponents(edgesTable, sourceColumn, targetColumn string) ([]ConnectedComponentResult, error) {
	sql := fmt.Sprintf(`
		SELECT node, component_id
		FROM graph_connected_components(
			'%s',
			'%s',
			'%s'
		)
		ORDER BY component_id, node
	`, edgesTable, sourceColumn, targetColumn)

	result, err := ga.client.Query(sql)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	results := make([]ConnectedComponentResult, 0)
	for result.Next() {
		var r ConnectedComponentResult
		if err := result.Scan(&r.Node, &r.ComponentID); err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}
