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

    // Lấy thông tin DB từ biến môi trường
    private static final String DB_URL = System.getenv("DB_URL");
    private static final String DB_USER = System.getenv("DB_USER");
    private static final String DB_PASS = System.getenv("DB_PASS");

    // Biến static giúp nhớ trạng thái, chỉ tạo bảng 1 lần duy nhất khi Lambda khởi động
    private static boolean isTableInitialized = false;

    // Hàm tự động dọn ổ
    private void initDatabase(Connection conn, Context context) {
        if (isTableInitialized) return;

        String createTableSql = 
            "CREATE TABLE IF NOT EXISTS books (" +
            "    id SERIAL PRIMARY KEY," +
            "    title VARCHAR(500)," +
            "    price VARCHAR(50)," +
            "    stock VARCHAR(100)," +
            "    url TEXT UNIQUE" +
            ");";
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql);
            context.getLogger().log("Khởi tạo DB: Đã kiểm tra và đảm bảo bảng 'books' tồn tại!");
            isTableInitialized = true;
        } catch (SQLException e) {
            context.getLogger().log("Lỗi khi tạo bảng: " + e.getMessage());
        }
    }

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
            context.getLogger().log("Không có tin nhắn SQS nào.");
            return "Bỏ qua";
        }

        // Câu lệnh UPSERT (Có thì update, chưa có thì Insert mới)
        String sql = "INSERT INTO books (title, price, stock, url) VALUES (?, ?, ?, ?) " +
                     "ON CONFLICT (url) DO UPDATE SET price = EXCLUDED.price, stock = EXCLUDED.stock;";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            
            // 1. Chạy hàm kiểm tra bảng đầu tiên
            initDatabase(conn, context);

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                context.getLogger().log("Bắt đầu xử lý " + event.getRecords().size() + " link...");

                // 2. Lặp qua từng link SQS gửi tới
                for (SQSMessage msg : event.getRecords()) {
                    String bookUrl = msg.getBody();

                    try {
                        // Cào Web
                        Document doc = Jsoup.connect(bookUrl).timeout(10000).get();
                        String title = doc.select("h1").text();
                        String price = doc.select(".price_color").text();
                        String stock = doc.select(".instock.availability").text().trim();

                        // Nạp đạn vào SQL
                        pstmt.setString(1, title);
                        pstmt.setString(2, price);
                        pstmt.setString(3, stock);
                        pstmt.setString(4, bookUrl);
                        
                        pstmt.addBatch();
                        context.getLogger().log("Đã cào: " + title);

                    } catch (Exception e) {
                        context.getLogger().log("Lỗi cào link " + bookUrl + ": " + e.getMessage());
                    }
                }

                // 3. Bóp cò: Ghi toàn bộ dữ liệu vào DB trong 1 nhịp
                int[] results = pstmt.executeBatch();
                context.getLogger().log("==> XONG! Đã lưu/cập nhật " + results.length + " bản ghi vào DB.");
            }

        } catch (SQLException e) {
            context.getLogger().log("Lỗi DB Tổng: " + e.getMessage());
            return "Thất bại";
        }

        return "Hoàn thành xuất sắc!";
    }
}