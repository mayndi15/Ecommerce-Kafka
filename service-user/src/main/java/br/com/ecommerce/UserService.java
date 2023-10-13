package br.com.ecommerce;

import br.com.ecommerce.services.ConsumerService;
import br.com.ecommerce.services.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class UserService implements ConsumerService<Order> {

    private final LocalDataBase dataBase;

    public UserService() throws SQLException {
        this.dataBase = new LocalDataBase("user_database");
        this.dataBase.createIfNotExists("create table Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200))");

    }

    public static void main(String[] args) {
        new ServiceRunner<>(UserService::new).start(1);
    }

    @Override
    public String getTopic() {
        return "ecommerce.new.order";
    }

    @Override
    public String getConsumerGroup() {
        return UserService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("----------------------------------------------");
        System.out.println("Processing new order, checking for new user...");
        System.out.println("value: " + record.value());

        var order = record.value().getPayload();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var uuid = UUID.randomUUID().toString();
        dataBase.update("insert into Users (uuid, email) values (?,?)", uuid, email);
    }

    private boolean isNewUser(String email) throws SQLException {
        var results = dataBase.query("select uuid from Users where email = ? limit 1", email);
        return !results.next();
    }
}

//sudo mount -o remount,exec /tmp