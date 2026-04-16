package com.example.producer;

import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SeederApplication implements CommandLineRunner {

    @Autowired
    private SqsTemplate sqsTemplate;

    // Thay bằng URL của hàng đợi SQS bạn tạo trên AWS Console
    private final String QUEUE_URL = "https://sqs.ap-southeast-2.amazonaws.com/510728439377/SQS";

    // ĐÂY LÀ HÀM MAIN MÀ SPRING BOOT ĐANG TÌM KIẾM
    public static void main(String[] args) {
        SpringApplication.run(SeederApplication.class, args);
    }

    @Override
    public void run(String... args) {
        System.out.println(">>> [SEEDER] Đang kết nối AWS SQS và gieo hạt...");
        
        try {
            for (int i = 1; i <= 50; i++) {
                String url = "https://books.toscrape.com/catalogue/page-" + i + ".html";
                
                // Bắn message thẳng lên Cloud
                sqsTemplate.send(to -> to.queue(QUEUE_URL).payload(url));
                
                System.out.println("   -> Đã bơm thành công: " + url);
            }
            System.out.println(">>> [SEEDER] Hoàn tất 50 link! App tự động tắt để tiết kiệm tiền.");
            
        } catch (Exception e) {
            System.err.println(">>> [LỖI] Không thể gửi lên SQS: " + e.getMessage());
        } finally {
            // Đảm bảo dù thành công hay lỗi, con app này cũng tự "sát sinh" khi làm xong việc
            System.exit(0); 
        }
    }
}