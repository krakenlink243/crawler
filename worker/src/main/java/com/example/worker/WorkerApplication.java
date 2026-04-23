package com.example.worker;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class WorkerApplication implements RequestHandler<Map<String, Object>, String> {

    private static final String DB_URL = System.getenv("123123");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASS = System.getenv("DB_PASS");
    private static boolean isTableInitialized = false;

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        
        // --- [BƯỚC 1] DEEP HEALTH CHECK: BẮT LỖI DB NGAY TẠI ĐÂY ---
        if (input != null && "health_check".equals(input.get("action"))) {
            context.getLogger().log(">>> WORKER: Dang kham suc khoe (Check DB)...");
            try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
                return "OK_ALIVE"; // URL dung thi moi tra ve cai nay
            } catch (Exception e) {
                context.getLogger().log(">>> WORKER HEALTH FAIL: " + e.getMessage());
                // Nem loi de GitHub Actions biet ma Rollback
                throw new RuntimeException("DB_ERROR: " + e.getMessage());
            }
        }

        // --- [BƯỚC 2] LOGIC XỬ LÝ SQS (GIỮ NGUYÊN CỦA ÔNG) ---
        List<Map<String, Object>> records = (List<Map<String, Object>>) input.get("Records");
        context.getLogger().log(">>> LAMBDA START: Nhan duoc " + (records != null ? records.size() : 0) + " tin nhan.");

        if (records == null || records.isEmpty()) return "Event trong rong";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            context.getLogger().log(">>> DB CONNECTED: " + DB_URL);

            // Kiem tra/Tao bang
            if (!isTableInitialized) {
                String createTableSql = "CREATE TABLE IF NOT EXISTS books (" +
                        "id SERIAL PRIMARY KEY, title VARCHAR(500), price VARCHAR(50), " +
                        "stock VARCHAR(100), url TEXT UNIQUE);";
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(createTableSql);
                    isTableInitialized = true;
                }
            }

            // Insert Batch
            String sql = "INSERT INTO books (title, price, stock, url) VALUES (?, ?, ?, ?) " +
                         "ON CONFLICT (url) DO UPDATE SET price = EXCLUDED.price, stock = EXCLUDED.stock;";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (Map<String, Object> msg : records) {
                    String bookUrl = (String) msg.get("body");
                    context.getLogger().log(">>> PROCESSING: " + bookUrl);

                    try {
                        Document doc = Jsoup.connect(bookUrl).timeout(10000).get();
                        pstmt.setString(1, doc.select("h1").text());
                        pstmt.setString(2, doc.select(".price_color").text());
                        pstmt.setString(3, doc.select(".instock.availability").text().trim());
                        pstmt.setString(4, bookUrl);
                        pstmt.addBatch();
                    } catch (Exception e) {
                        context.getLogger().log(">>> JSOUP ERROR: " + e.getMessage());
                    }
                }
                int[] results = pstmt.executeBatch();
                context.getLogger().log(">>> DB SUCCESS: Ghi " + results.length + " ban ghi.");
            }
        } catch (SQLException e) {
            context.getLogger().log(">>> SQL ERROR: " + e.getMessage());
            return "Loi SQL: " + e.getMessage();
        } catch (Exception e) {
            return "Loi khac: " + e.toString();
        }
        return "Hoan thanh";
    }
}