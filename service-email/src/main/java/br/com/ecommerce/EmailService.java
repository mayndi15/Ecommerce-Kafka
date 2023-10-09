package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaConsumers;
import br.com.ecommerce.kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailService();
        try (var service = new KafkaConsumers<>(EmailService.class.getSimpleName(),
                "ecommerce.send.email",
                emailService::parse,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("-------------------------------------------");
        System.out.println("Send email...");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent!");
    }
}
