package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;
import br.com.ecommerce.services.ConsumerService;
import br.com.ecommerce.services.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailOrderService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ecommerce.new.order";
    }

    @Override
    public String getConsumerGroup() {
        return EmailOrderService.class.getSimpleName();
    }

    private final KafkaProducers<String> emailProducer = new KafkaProducers<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var message = record.value();
        var order = message.getPayload();
        var correlationId = message.getId().continueWith(EmailOrderService.class.getSimpleName());
        var email = order.getEmail();
        var emailCode = "Thank you for your order! We are processing your order!";

        System.out.println("----------------------------------------");
        System.out.println("Processing new order, preparing email...");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());

        emailProducer.send("ecommerce.send.email", email, emailCode, correlationId);
    }
}
