package com.example.worker;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.*;
import java.util.*;

public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private static final String DB_URL = System.getenv("DB_URL");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASS = System.getenv("DB_PASS");
    
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        // --- 1. CHỐT CHẶN HEALTH CHECK QUA URL ---
        String path = (String) input.getOrDefault("rawPath", "/");
        if ("/health".equals(path)) {
            context.getLogger().log(">>> API PING: He thong dang goi /health");
            return createResponse(200, Map.of("status", "API_ALIVE_AND_KICKING"));
        }

        context.getLogger().log(">>> API Lambda: Dang lay du lieu cho ong giao...");
        List<Map<String, String>> books = new ArrayList<>();

        try {
            Class.forName("org.postgresql.Driver");

            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM books ORDER BY id DESC LIMIT 50")) {

                while (rs.next()) {
                    Map<String, String> book = new HashMap<>();
                    book.put("id", rs.getString("id"));
                    book.put("title", rs.getString("title"));
                    book.put("price", rs.getString("price"));
                    book.put("stock", rs.getString("stock"));
                    book.put("url", rs.getString("url"));
                    books.add(book);
                }
            }
        } catch (Exception e) {
            context.getLogger().log(">>> LOI DB: " + e.getMessage());
            return createResponse(500, Map.of("error", e.getMessage()));
        }

        return createResponse(200, books);
    }

    private Map<String, Object> createResponse(int statusCode, Object body) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json; charset=utf-8");
        headers.put("Access-Control-Allow-Origin", "*"); 
        
        response.put("headers", headers);
        response.put("body", gson.toJson(body));
        return response;
    }
}