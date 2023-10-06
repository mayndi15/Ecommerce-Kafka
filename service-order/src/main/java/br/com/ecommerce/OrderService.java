package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class OrderService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderProducer = new KafkaProducers<Order>()) {
            try (var emailProducer = new KafkaProducers<String>()) {
                for (var i = 0; i < 5; i++) {

                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                    var order = new Order(userId, orderId, amount);
                    orderProducer.send("ecommerce.new.order", userId, order);

                    var email = "Thank you for your order! We are processing your order!";
                    emailProducer.send("ecommerce.send.email", userId, email);
                }
            }
        }
    }
}
