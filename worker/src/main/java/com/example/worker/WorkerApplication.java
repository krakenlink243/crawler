package com.example.worker;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class WorkerApplication implements RequestHandler<SQSEvent, String> {

    @Override
    public String handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Worker đã thức dậy! Nhận được " + event.getRecords().size() + " tin nhắn.");

        for (SQSMessage msg : event.getRecords()) {
            String bookUrl = msg.getBody(); // Đây chính là cái link link Producer ném vào
            
            try {
                context.getLogger().log("Đang xử lý cuốn sách: " + bookUrl);

                // 1. Cào chi tiết
                Document doc = Jsoup.connect(bookUrl)
                        .timeout(10000)
                        .get();

                // 2. Bóc tách dữ liệu (Ví dụ: Tên sách và Giá)
                String title = doc.select("h1").text();
                String price = doc.select(".price_color").text();
                String stock = doc.select(".instock.availability").text().trim();

                // 3. In ra log (Sau này ông sẽ ném chỗ này vào Database như RDS hoặc DynamoDB)
                context.getLogger().log(String.format("===> KẾT QUẢ: [%s] - Giá: %s - Tình trạng: %s", title, price, stock));

            } catch (Exception e) {
                context.getLogger().log("Lỗi khi cào link " + bookUrl + ": " + e.getMessage());
                // Lưu ý: Nếu ném Exception ở đây, SQS sẽ hiểu là xử lý lỗi và gửi lại tin nhắn này sau.
            }
        }

        return "Đã xử lý xong batch!";
    }
}