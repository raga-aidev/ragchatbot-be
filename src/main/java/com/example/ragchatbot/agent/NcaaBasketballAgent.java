package com.example.ragchatbot.agent;

import com.example.ragchatbot.service.BigQuerySchemaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Component
public class NcaaBasketballAgent {

    @Value("${google.api.key}")
    private String apiKey;

    @Value("${gcp.bigquery.dataset}")
    private String datasetName;

    @Value("${gcp.bigquery.schema}")
    private String schemaName;

    private final BigQuerySchemaService schemaService;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public NcaaBasketballAgent(BigQuerySchemaService schemaService) {
        this.schemaService = schemaService;
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
    }

    public String generateSql(String userQuery) throws IOException, InterruptedException {
        String schemaContext = schemaService.getSchemaContext();
        
        String systemPrompt = buildSystemPrompt(schemaContext);
        String userPrompt = "User query: " + userQuery + "\n\nGenerate a BigQuery SQL query to answer this question. " +
                           "Return ONLY the SQL query, no explanations or markdown formatting.";

        String requestBody = buildGeminiRequest(systemPrompt, userPrompt);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key=" + apiKey))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        String sql = extractSqlFromResponse(response.body());
        
        // Validate SQL safety
        validateSqlSafety(sql);
        
        return sql;
    }

    private String buildSystemPrompt(String schemaContext) {
        return "You are a BigQuery SQL expert specializing in the NCAA basketball dataset.\n\n" +
               "Dataset: " + datasetName + "." + schemaName + "\n\n" +
               "Schema Information:\n" + schemaContext + "\n\n" +
               "Rules:\n" +
               "1. Generate valid BigQuery Standard SQL queries only\n" +
               "2. Use proper table references: `bigquery-public-data.ncaa_basketball.table_name`\n" +
               "3. Return ONLY the SQL query, no markdown code blocks, no explanations\n" +
               "4. Ensure queries are efficient and use appropriate WHERE clauses\n" +
               "5. Use LIMIT clauses when appropriate to avoid large result sets\n" +
               "6. Format dates properly using BigQuery date functions\n";
    }

    private String buildGeminiRequest(String systemPrompt, String userPrompt) throws IOException {
        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> content = new HashMap<>();
        Map<String, String> part = new HashMap<>();
        part.put("text", systemPrompt + "\n\n" + userPrompt);
        content.put("parts", Arrays.asList(part));
        requestMap.put("contents", Arrays.asList(content));
        
        return objectMapper.writeValueAsString(requestMap);
    }

    private String extractSqlFromResponse(String responseBody) throws IOException {
        try {
            JsonNode rootNode = objectMapper.readTree(responseBody);
            JsonNode candidates = rootNode.get("candidates");
            if (candidates != null && candidates.isArray() && candidates.size() > 0) {
                JsonNode content = candidates.get(0).get("content");
                if (content != null) {
                    JsonNode parts = content.get("parts");
                    if (parts != null && parts.isArray() && parts.size() > 0) {
                        JsonNode textNode = parts.get(0).get("text");
                        if (textNode != null) {
                            String text = textNode.asText();
                            // Remove markdown code blocks if present
                            text = text.replaceAll("```sql", "").replaceAll("```", "").trim();
                            // Extract SQL if wrapped in backticks
                            if (text.contains("`")) {
                                int backtickStart = text.indexOf("`");
                                int backtickEnd = text.indexOf("`", backtickStart + 1);
                                if (backtickEnd != -1) {
                                    text = text.substring(backtickStart + 1, backtickEnd);
                                }
                            }
                            return text.trim();
                        }
                    }
                }
            }
        } catch (Exception e) {
            // Fallback: try simple string extraction
            int startIdx = responseBody.indexOf("\"text\":\"");
            if (startIdx != -1) {
                startIdx += 8;
                int endIdx = responseBody.indexOf("\"", startIdx);
                if (endIdx != -1) {
                    String text = responseBody.substring(startIdx, endIdx);
                    text = text.replaceAll("```sql", "").replaceAll("```", "").trim();
                    return text.trim();
                }
            }
        }
        return responseBody.trim();
    }

    private void validateSqlSafety(String sql) {
        String upperSql = sql.toUpperCase();
        String[] dangerousKeywords = {"DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE"};
        
        for (String keyword : dangerousKeywords) {
            if (upperSql.contains(keyword)) {
                throw new IllegalArgumentException("Unsafe SQL detected: " + keyword + " operations are not allowed");
            }
        }
    }
}

