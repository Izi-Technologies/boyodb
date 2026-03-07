package io.boyodb.client;

import java.util.*;

/**
 * Result of a SQL query.
 */
public class QueryResult implements Iterable<Map<String, Object>> {
    private List<String> columns = new ArrayList<>();
    private List<Map<String, Object>> rows = new ArrayList<>();
    private int segmentsScanned;
    private long dataSkippedBytes;

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }

    public int getRowCount() {
        return rows.size();
    }

    public int getSegmentsScanned() {
        return segmentsScanned;
    }

    public void setSegmentsScanned(int segmentsScanned) {
        this.segmentsScanned = segmentsScanned;
    }

    public long getDataSkippedBytes() {
        return dataSkippedBytes;
    }

    public void setDataSkippedBytes(long dataSkippedBytes) {
        this.dataSkippedBytes = dataSkippedBytes;
    }

    /**
     * Get a row by index.
     */
    public Map<String, Object> get(int index) {
        return rows.get(index);
    }

    /**
     * Check if result is empty.
     */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        return rows.iterator();
    }

    /**
     * Get the first row, or null if empty.
     */
    public Map<String, Object> first() {
        return rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * Get a column value from the first row.
     */
    @SuppressWarnings("unchecked")
    public <T> T getScalar(String column) {
        Map<String, Object> row = first();
        return row == null ? null : (T) row.get(column);
    }

    /**
     * Get a column as a list of values.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> getColumn(String column) {
        List<T> values = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            values.add((T) row.get(column));
        }
        return values;
    }
}
