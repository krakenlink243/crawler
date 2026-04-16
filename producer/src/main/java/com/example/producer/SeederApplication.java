package com.example.producer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SeederApplication implements RequestHandler<Map<String, Object>, String> {

    private final String QUEUE_URL = System.getenv("SQS_QUEUE_URL");
    private final SqsClient sqsClient = SqsClient.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        context.getLogger().log("Bắt đầu cào từ trang 1 đến 100...");
        int totalLinksFound = 0;

        try {
            for (int i = 1; i <= 50; i++) {
                String pageUrl = "https://books.toscrape.com/catalogue/page-" + i + ".html";
                context.getLogger().log("Đang quét trang: " + pageUrl);

                Document doc = Jsoup.connect(pageUrl).get();
                Elements bookLinks = doc.select("h3 a");

                List<SendMessageBatchRequestEntry> entries = new ArrayList<>();

                for (Element link : bookLinks) {
                    // Chuyển link tương đối thành tuyệt đối
                    String absoluteUrl = "https://books.toscrape.com/catalogue/" + link.attr("href");
                    
                    // Tạo entry để gửi theo batch (tối đa 10 tin/lần để tiết kiệm request)
                    entries.add(SendMessageBatchRequestEntry.builder()
                            .id(UUID.randomUUID().toString())
                            .messageBody(absoluteUrl)
                            .build());

                    if (entries.size() == 10) {
                        sendBatch(entries);
                        totalLinksFound += entries.size();
                        entries.clear();
                    }
                }

                // Gửi nốt những tin còn dư trong list
                if (!entries.isEmpty()) {
                    sendBatch(entries);
                    totalLinksFound += entries.size();
                }
            }
        } catch (Exception e) {
            context.getLogger().log("Lỗi nghiêm trọng: " + e.getMessage());
            return "Thất bại!";
        }

        return "Hoàn thành! Tổng cộng đã ném " + totalLinksFound + " link vào SQS.";
    }

    private void sendBatch(List<SendMessageBatchRequestEntry> entries) {
        SendMessageBatchRequest batchRequest = SendMessageBatchRequest.builder()
                .queueUrl(QUEUE_URL)
                .entries(entries)
                .build();
        sqsClient.sendMessageBatch(batchRequest);
    }
}