package com.example.ragchatbot.service;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ChatResponseFormatter {

    public Map<String, Object> formatResponse(String userQuery, List<String> columnNames, List<List<Object>> rows) {
        Map<String, Object> response = new HashMap<>();
        
        if (rows == null || rows.isEmpty()) {
            response.put("message", "No data found for your query.");
            return response;
        }

        // Determine if this should be a graph or table
        boolean isGraphData = isGraphData(columnNames, rows);
        
        if (isGraphData) {
            Map<String, Object> graphData = createGraphData(columnNames, rows);
            response.put("message", "Here's the visualization of your data:");
            response.put("graphData", graphData);
        } else {
            Map<String, Object> tableData = createTableData(columnNames, rows);
            response.put("message", "Here are the results:");
            response.put("tableData", tableData);
        }

        return response;
    }

    private boolean isGraphData(List<String> columnNames, List<List<Object>> rows) {
        if (columnNames == null || columnNames.size() < 2 || rows.size() < 2) {
            return false;
        }

        // Check if we have numeric columns suitable for graphing
        int numericColumnCount = 0;
        for (int i = 0; i < columnNames.size(); i++) {
            if (isNumericColumn(rows, i)) {
                numericColumnCount++;
            }
        }

        // If we have at least 2 columns and at least one numeric column, consider it graph data
        return numericColumnCount > 0 && columnNames.size() >= 2;
    }

    private boolean isNumericColumn(List<List<Object>> rows, int columnIndex) {
        if (rows.isEmpty()) {
            return false;
        }

        int numericCount = 0;
        for (List<Object> row : rows) {
            if (columnIndex < row.size()) {
                Object value = row.get(columnIndex);
                if (value instanceof Number) {
                    numericCount++;
                } else if (value != null) {
                    try {
                        Double.parseDouble(value.toString());
                        numericCount++;
                    } catch (NumberFormatException e) {
                        // Not numeric
                    }
                }
            }
        }

        // Consider numeric if more than 50% of values are numeric
        return numericCount > rows.size() * 0.5;
    }

    private Map<String, Object> createGraphData(List<String> columnNames, List<List<Object>> rows) {
        Map<String, Object> graphData = new HashMap<>();
        
        // Find first numeric column for Y axis
        int yColumnIndex = -1;
        for (int i = 0; i < columnNames.size(); i++) {
            if (isNumericColumn(rows, i)) {
                yColumnIndex = i;
                break;
            }
        }

        if (yColumnIndex == -1) {
            // Fallback to table if no numeric column found
            return createTableData(columnNames, rows);
        }

        // Use first column as X axis (or index if first column is also numeric)
        int xColumnIndex = (yColumnIndex == 0) ? -1 : 0;

        List<Object> xData = new ArrayList<>();
        List<Object> yData = new ArrayList<>();
        List<String> labels = new ArrayList<>();

        for (List<Object> row : rows) {
            if (xColumnIndex >= 0 && xColumnIndex < row.size()) {
                xData.add(row.get(xColumnIndex));
            } else {
                xData.add(xData.size() + 1); // Use index
            }
            
            if (yColumnIndex < row.size()) {
                Object yValue = row.get(yColumnIndex);
                if (yValue instanceof Number) {
                    yData.add(yValue);
                } else if (yValue != null) {
                    try {
                        yData.add(Double.parseDouble(yValue.toString()));
                    } catch (NumberFormatException e) {
                        yData.add(0);
                    }
                } else {
                    yData.add(0);
                }
            }

            // Create label from first few columns
            StringBuilder label = new StringBuilder();
            for (int i = 0; i < Math.min(3, columnNames.size()); i++) {
                if (i < row.size() && row.get(i) != null) {
                    if (label.length() > 0) label.append(" - ");
                    label.append(columnNames.get(i)).append(": ").append(row.get(i));
                }
            }
            labels.add(label.toString());
        }

        // Determine chart type based on data
        String chartType = determineChartType(xData, yData);

        graphData.put("chartType", chartType);
        graphData.put("x", xData);
        graphData.put("y", yData);
        graphData.put("labels", labels);
        graphData.put("xLabel", xColumnIndex >= 0 ? columnNames.get(xColumnIndex) : "Index");
        graphData.put("yLabel", columnNames.get(yColumnIndex));

        return graphData;
    }

    private String determineChartType(List<Object> xData, List<Object> yData) {
        // Simple heuristic: use line for time-series-like data, bar for categorical
        if (xData.size() > 10) {
            return "line";
        } else {
            return "bar";
        }
    }

    private Map<String, Object> createTableData(List<String> columnNames, List<List<Object>> rows) {
        Map<String, Object> tableData = new HashMap<>();
        tableData.put("columns", columnNames);
        
        List<List<Object>> formattedRows = new ArrayList<>();
        for (List<Object> row : rows) {
            List<Object> formattedRow = new ArrayList<>();
            for (int i = 0; i < columnNames.size(); i++) {
                if (i < row.size()) {
                    formattedRow.add(row.get(i));
                } else {
                    formattedRow.add(null);
                }
            }
            formattedRows.add(formattedRow);
        }
        
        tableData.put("rows", formattedRows);
        return tableData;
    }
}

