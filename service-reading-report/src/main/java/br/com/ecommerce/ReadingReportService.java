package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaConsumers;
import br.com.ecommerce.kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportService {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var reportService = new ReadingReportService();
        try (var service = new KafkaConsumers<>(ReadingReportService.class.getSimpleName(),
                "ecommerce.user.generate.reading.report",
                reportService::parse,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing report for " + record.value());
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());

        var message = record.value();
        var user = message.getPayload();
        var target = new File(user.getReportPath());

        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());
        System.out.println("File created: " + target.getAbsolutePath());
    }
}
