package com.example.worker;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class WorkerApplication implements RequestHandler<Map<String, Object>, String> {

    private static final String DB_URL = System.getenv("123");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASS = System.getenv("DB_PASS");

    private static boolean isTableInitialized = false;

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        // --- 1. CHẶN ĐẦU: KHÁM SỨC KHỎE ---
        if (input != null && "health_check".equals(input.get("action"))) {
            context.getLogger().log(">>> WORKER PING: Nhan lenh health check. Tra ve OK_ALIVE.");
            return "OK_ALIVE";
        }

        // --- 2. LOG ĐẦU TIÊN: ĐỂ BIẾT LÀ CODE ĐÃ CHẠY VÀO ĐÂY ---
        List<Map<String, Object>> records = (List<Map<String, Object>>) input.get("Records");
        context.getLogger().log(">>> LAMBDA START: Nhan duoc " + (records != null ? records.size() : 0) + " tin nhan.");

        if (records == null || records.isEmpty()) {
            return "Event trong rong";
        }

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            context.getLogger().log(">>> DB CONNECTED: Ket noi thanh cong toi " + DB_URL);

            if (!isTableInitialized) {
                String createTableSql = "CREATE TABLE IF NOT EXISTS books (" +
                        "id SERIAL PRIMARY KEY, title VARCHAR(500), price VARCHAR(50), " +
                        "stock VARCHAR(100), url TEXT UNIQUE);";
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(createTableSql);
                    isTableInitialized = true;
                    context.getLogger().log(">>> TABLE CHECK: Bang 'books' da san sang.");
                }
            }

            String sql = "INSERT INTO books (title, price, stock, url) VALUES (?, ?, ?, ?) " +
                         "ON CONFLICT (url) DO UPDATE SET price = EXCLUDED.price, stock = EXCLUDED.stock;";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (Map<String, Object> msg : records) {
                    // Bóc tách body từ Map SQS Record
                    String bookUrl = (String) msg.get("body");
                    context.getLogger().log(">>> PROCESSING: Dang cao link: " + bookUrl);

                    try {
                        Document doc = Jsoup.connect(bookUrl).timeout(10000).get();
                        String title = doc.select("h1").text();
                        String price = doc.select(".price_color").text();
                        String stock = doc.select(".instock.availability").text().trim();

                        pstmt.setString(1, title);
                        pstmt.setString(2, price);
                        pstmt.setString(3, stock);
                        pstmt.setString(4, bookUrl);
                        pstmt.addBatch();
                        
                        context.getLogger().log(">>> JSOUP SUCCESS: " + title);
                    } catch (Exception e) {
                        context.getLogger().log(">>> JSOUP ERROR tại " + bookUrl + ": " + e.getMessage());
                    }
                }
                int[] results = pstmt.executeBatch();
                context.getLogger().log(">>> DB SUCCESS: Ghi thanh cong " + results.length + " ban ghi.");
            }

        } catch (SQLException e) {
            context.getLogger().log(">>> DATABASE CRITICAL ERROR: " + e.getMessage());
            return "Loi SQL: " + e.getMessage();
        } catch (Exception e) {
            context.getLogger().log(">>> UNKNOWN ERROR: " + e.toString());
            return "Loi khac: " + e.toString();
        }

        return "Hoan thanh";
    }
}