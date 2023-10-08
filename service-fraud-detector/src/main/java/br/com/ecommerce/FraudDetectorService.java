package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaConsumers;
import br.com.ecommerce.kafka.KafkaProducers;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

    private final KafkaProducers<Order> orderProducer = new KafkaProducers<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing new order, checking for fraud...");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        var order = record.value();

        if (isFraud(order)) {
            //pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!");
            orderProducer.send("ecommerce.rejected.order", order.getEmail(), order);
        } else {
            System.out.println("Approved: Order processed! " + order);
            orderProducer.send("ecommerce.approved.order", order.getEmail(), order);
        }
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
