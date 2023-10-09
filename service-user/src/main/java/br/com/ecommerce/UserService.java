package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaConsumers;
import br.com.ecommerce.kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class UserService {

    private final Connection connection;

    public UserService() throws SQLException {
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
        var userService = new UserService();
        try (var service = new KafkaConsumers<>(UserService.class.getSimpleName(),
                "ecommerce.new.order",
                userService::parse,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("----------------------------------------------");
        System.out.println("Processing new order, checking for new user...");
        System.out.println("value: " + record.value());

        var order = record.value().getPayload();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("insert into Users (uuid, email) values (?,?)");
        insert.setString(1, UUID.randomUUID().toString());
        insert.setString(2, email);
        insert.execute();
        System.out.println("User " + email + " added successfully!");
    }

    private boolean isNewUser(String email) throws SQLException {
        var emailPersist = connection.prepareStatement("select uuid from Users where email = ? limit 1");
        emailPersist.setString(1, email);
        var result = emailPersist.executeQuery();

        return !result.next();
    }
}

//sudo mount -o remount,exec /tmp