package com.example.worker;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;

public class WorkerApplication implements RequestHandler<SQSEvent, String> {

    private static final String DB_URL = System.getenv("DB_URL");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASS = System.getenv("DB_PASS");

    private static boolean isTableInitialized = false;

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        // 1. LOG ĐẦU TIÊN: ĐỂ BIẾT LÀ CODE ĐÃ CHẠY VÀO ĐÂY
        context.getLogger().log(">>> LAMBDA START: Nhan duoc " + (event.getRecords() != null ? event.getRecords().size() : 0) + " tin nhan.");

        if (event.getRecords() == null || event.getRecords().isEmpty()) {
            return "Event trong rong";
        }

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            context.getLogger().log(">>> DB CONNECTED: Ket noi thanh cong toi " + DB_URL);

            // 2. Kiểm tra/Tạo bảng
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

            // 3. Xử lý từng Record
            String sql = "INSERT INTO books (title, price, stock, url) VALUES (?, ?, ?, ?) " +
                         "ON CONFLICT (url) DO UPDATE SET price = EXCLUDED.price, stock = EXCLUDED.stock;";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (SQSMessage msg : event.getRecords()) {
                    String bookUrl = msg.getBody();
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