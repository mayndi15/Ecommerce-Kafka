package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaConsumers;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudService = new FraudDetectorService();
        try (var service = new KafkaConsumers<>(FraudDetectorService.class.getSimpleName(),
                "ecommerce.new.order",
                fraudService::parse,
                Order.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("-------------------------------------------");
        System.out.println("Processing new order, checking for fraud...");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        System.out.println("Order processed!");
    }
}
