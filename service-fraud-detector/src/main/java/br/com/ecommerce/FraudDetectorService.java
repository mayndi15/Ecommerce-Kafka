package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;
import br.com.ecommerce.services.ConsumerService;
import br.com.ecommerce.services.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private final LocalDataBase dataBase;

    public FraudDetectorService() throws SQLException {
        this.dataBase = new LocalDataBase("frauds_database");
        this.dataBase.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean)");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ecommerce.new.order";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    private final KafkaProducers<Order> orderProducer = new KafkaProducers<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing new order, checking for fraud...");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());

        var message = record.value();
        var order = message.getPayload();

        if (wasProcessed(order)) {
            System.out.println("Order: " + order.getOrderId() + " was already processed");
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        if (isFraud(order)) {
            dataBase.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());

            //pretending that the fraud happens when the amount is >= 4500
            System.out.println("Order is a fraud!");
            orderProducer.send("ecommerce.rejected.order", order.getEmail(), order, message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
        } else {
            dataBase.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());

            System.out.println("Approved: Order processed! " + order);
            orderProducer.send("ecommerce.approved.order", order.getEmail(), order, message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = dataBase.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
