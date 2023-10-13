package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class OrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderProducer = new KafkaProducers<Order>()) {
            for (var i = 0; i < 5; i++) {
                var correlationId = new CorrelationId(OrderService.class.getSimpleName());
                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                var email = Math.random() + "@email.com";

                var order = new Order(orderId, amount, email);
                orderProducer.send("ecommerce.new.order", email, order, correlationId);
            }
        }
    }
}
