package com.example.worker;

import io.awspring.cloud.sqs.annotation.SqsListener;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

    // Tự động lắng nghe khi có tin nhắn mới trong SQS
    @SqsListener("books-queue")
    public void listen(String url) {
        System.out.println("[AWS WORKER] Đang xử lý link từ SQS: " + url);
        try {
            Document doc = Jsoup.connect(url).userAgent("Mozilla/5.0").get();
            String title = doc.title();
            
            // Ở đây bạn sẽ lưu vào Amazon RDS (PostgreSQL)
            System.out.println("   -> Kết quả: " + title);
            
            // Giả lập xử lý nặng
            Thread.sleep(1000); 
        } catch (Exception e) {
            System.err.println("Lỗi cào AWS: " + e.getMessage());
        }
    }
}