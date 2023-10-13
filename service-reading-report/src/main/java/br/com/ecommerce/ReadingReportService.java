package br.com.ecommerce;

import br.com.ecommerce.services.ConsumerService;
import br.com.ecommerce.services.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService implements ConsumerService<User> {

    private static final Path SOURCE = new File("target/report.txt").toPath();

    public static void main(String[] args) {
        new ServiceRunner<>(ReadingReportService::new).start(4);
    }

    @Override
    public String getTopic() {
        return "ecommerce.user.generate.reading.report";
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("---------------------------------------");
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
