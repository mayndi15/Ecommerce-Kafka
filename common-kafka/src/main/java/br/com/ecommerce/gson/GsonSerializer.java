package br.com.ecommerce.gson;

import br.com.ecommerce.kafka.Message;
import br.com.ecommerce.kafka.MessageAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class GsonSerializer<T> implements Serializer<T> {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

    @Override
    public byte[] serialize(String s, T object) {
        return gson.toJson(object).getBytes();
    }
}
