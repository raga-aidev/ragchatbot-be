package com.example.ragchatbot.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class BigQuerySchemaService {

    @Value("${gcp.bigquery.dataset}")
    private String datasetName;

    @Value("${gcp.bigquery.schema}")
    private String schemaName;

    private BigQuery bigQuery;
    private String cachedSchemaContext;

    public BigQuerySchemaService() {
        this.bigQuery = BigQueryOptions.getDefaultInstance().getService();
    }

    public String getSchemaContext() {
        if (cachedSchemaContext == null) {
            cachedSchemaContext = buildSchemaContext();
        }
        return cachedSchemaContext;
    }

    private String buildSchemaContext() {
        StringBuilder schemaContext = new StringBuilder();
        schemaContext.append("BigQuery Dataset: ").append(datasetName).append(".").append(schemaName).append("\n\n");
        schemaContext.append("Available Tables and Schemas:\n\n");

        try {
            DatasetId datasetId = DatasetId.of(schemaName, datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            if (dataset == null) {
                return "Dataset not found: " + datasetName + "." + schemaName;
            }

            List<String> tableNames = new ArrayList<>();
            for (Table table : dataset.list().iterateAll()) {
                tableNames.add(table.getTableId().getTable());
            }

            // Focus on key NCAA basketball tables
            String[] keyTables = {"mbb_games", "mbb_teams", "mbb_players_games_sr", "mbb_historical_teams_games", 
                                  "mbb_historical_tournament_games", "mbb_historical_team_seasons"};
            
            for (String tableName : keyTables) {
                if (tableNames.contains(tableName)) {
                    TableId tableId = TableId.of(schemaName, datasetName, tableName);
                    Table table = bigQuery.getTable(tableId);
                    if (table != null) {
                        schemaContext.append(formatTableSchema(table));
                    }
                }
            }
        } catch (Exception e) {
            schemaContext.append("Error retrieving schemas: ").append(e.getMessage());
        }

        return schemaContext.toString();
    }

    private String formatTableSchema(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("Table: ").append(table.getTableId().getTable()).append("\n");
        
        TableDefinition definition = table.getDefinition();
        if (definition instanceof StandardTableDefinition) {
            StandardTableDefinition stdDef = (StandardTableDefinition) definition;
            Schema schema = stdDef.getSchema();
            
            if (schema != null && schema.getFields() != null) {
                sb.append("Columns:\n");
                for (Field field : schema.getFields()) {
                    sb.append("  - ").append(field.getName())
                      .append(" (").append(field.getType().toString()).append(")");
                    if (field.getDescription() != null && !field.getDescription().isEmpty()) {
                        sb.append(": ").append(field.getDescription());
                    }
                    sb.append("\n");
                }
            }
        }
        sb.append("\n");
        return sb.toString();
    }
}

