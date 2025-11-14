package com.example.ragchatbot.controller;

import com.example.ragchatbot.agent.NcaaBasketballAgent;
import com.example.ragchatbot.service.BigQueryExecutionService;
import com.example.ragchatbot.service.ChatResponseFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "http://localhost:4200")
public class ChatController {

    @Autowired
    private NcaaBasketballAgent agent;

    @Autowired
    private BigQueryExecutionService bigQueryService;

    @Autowired
    private ChatResponseFormatter formatter;

    @PostMapping("/chat")
    public ResponseEntity<Map<String, Object>> chat(@RequestBody Map<String, String> request) {
        try {
            String query = request.get("query");
            if (query == null || query.trim().isEmpty()) {
                Map<String, Object> errorResponse = new HashMap<>();
                errorResponse.put("message", "Query cannot be empty");
                return ResponseEntity.badRequest().body(errorResponse);
            }

            // Generate SQL using ADK agent
            String sql = agent.generateSql(query);

            // Execute query
            List<String> columnNames = bigQueryService.getColumnNames(sql);
            List<List<Object>> rows = bigQueryService.executeQuery(sql);

            // Format response
            Map<String, Object> response = formatter.formatResponse(query, columnNames, rows);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("message", "Error processing query: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
}

