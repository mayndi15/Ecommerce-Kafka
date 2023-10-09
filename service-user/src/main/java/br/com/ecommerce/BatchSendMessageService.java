package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaConsumers;
import br.com.ecommerce.kafka.KafkaProducers;
import br.com.ecommerce.kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {
    private final Connection connection;

    public BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:user_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute(
                    "create table Users (" +
                            "uuid varchar(200) primary key," +
                            "email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaConsumers<>(BatchSendMessageService.class.getSimpleName(),
                "ecommerce.send.message.to.all.users",
                batchService::parse,
                Map.of())) {
            service.run();
        }
    }

    private final KafkaProducers<User> userProducer = new KafkaProducers<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException {
        var message = record.value();

        System.out.println("----------------------------------------------");
        System.out.println("Processing new batch...");
        System.out.println("Topic: " + message.getPayload());

        for (User user : getAllUsers()) {
            userProducer.sendAsync(message.getPayload(), user.getUuid(), user, message.getId().continueWith(BatchSendMessageService.class.getSimpleName()));
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}
