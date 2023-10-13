package br.com.ecommerce;

import br.com.ecommerce.kafka.KafkaProducers;
import br.com.ecommerce.services.ConsumerService;
import br.com.ecommerce.services.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BatchSendMessageService implements ConsumerService<String> {
    private final LocalDataBase dataBase;

    public BatchSendMessageService() throws SQLException {
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
        return "ecommerce.send.message.to.all.users";
    }

    @Override
    public String getConsumerGroup() {
        return BatchSendMessageService.class.getSimpleName();
    }

    private final KafkaProducers<User> userProducer = new KafkaProducers<>();

    public void parse(ConsumerRecord<String, Message<String>> record) throws SQLException {
        var message = record.value();

        System.out.println("------------------------------");
        System.out.println("Processing new batch...");
        System.out.println("Topic: " + message.getPayload());

        for (User user : getAllUsers()) {
            userProducer.sendAsync(message.getPayload(), user.getUuid(), user, message.getId()
                    .continueWith(BatchSendMessageService.class.getSimpleName()));
        }
    }

    private List<User> getAllUsers() throws SQLException {
        var results = dataBase.query("select uuid from Users");
        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}
