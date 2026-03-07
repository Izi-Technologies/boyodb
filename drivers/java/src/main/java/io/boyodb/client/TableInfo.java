package io.boyodb.client;

/**
 * Table metadata information.
 */
public class TableInfo {
    private String database;
    private String name;
    private String schemaJson;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchemaJson() {
        return schemaJson;
    }

    public void setSchemaJson(String schemaJson) {
        this.schemaJson = schemaJson;
    }

    public String getFullName() {
        return database + "." + name;
    }

    @Override
    public String toString() {
        return getFullName();
    }
}
