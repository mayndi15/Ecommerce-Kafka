package br.com.ecommerce.services;

import br.com.ecommerce.kafka.KafkaConsumers;

import java.util.Map;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() throws Exception {
        var service = factory.create();
        try (var consumer = new KafkaConsumers<>(
                service.getConsumerGroup(),
                service.getTopic(),
                service::parse, Map.of())) {
            consumer.run();
        }
        return null;
    }
}
