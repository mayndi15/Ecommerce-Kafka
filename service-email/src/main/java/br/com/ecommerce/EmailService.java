package br.com.ecommerce;

import br.com.ecommerce.services.ConsumerService;
import br.com.ecommerce.services.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {
        new ServiceRunner<>(EmailService::new).start(4);
    }

    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    public String getTopic() {
        return "ecommerce.send.email";
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("----------------------------------------");
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
