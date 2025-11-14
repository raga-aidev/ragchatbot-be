package com.example.ragchatbot.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class BigQueryExecutionService {

    private final BigQuery bigQuery;

    public BigQueryExecutionService() {
        this.bigQuery = BigQueryOptions.getDefaultInstance().getService();
    }

    public List<List<Object>> executeQuery(String sql) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
                .setUseLegacySql(false)
                .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete
        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        }
        var error = queryJob.getStatus().getError();
        if (error != null) {
            throw new RuntimeException("Query failed: " + error.toString());
        }

        TableResult result = queryJob.getQueryResults();
        
        List<List<Object>> rows = new ArrayList<>();
        result.iterateAll().forEach(row -> {
            List<Object> rowData = new ArrayList<>();
            row.forEach(fieldValue -> rowData.add(fieldValue.getValue()));
            rows.add(rowData);
        });

        return rows;
    }

    public List<String> getColumnNames(String sql) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
                .setUseLegacySql(false)
                .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        }
        var error = queryJob.getStatus().getError();
        if (error != null) {
            throw new RuntimeException("Query failed: " + error.toString());
        }

        TableResult result = queryJob.getQueryResults();
        List<String> columnNames = new ArrayList<>();
        
        var schema = result.getSchema();
        if (schema != null && schema.getFields() != null) {
            schema.getFields().forEach(field -> columnNames.add(field.getName()));
        }

        return columnNames;
    }
}

