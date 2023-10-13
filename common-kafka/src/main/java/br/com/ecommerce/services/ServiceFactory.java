package br.com.ecommerce.services;

public interface ServiceFactory<T> {

    ConsumerService<T> create() throws Exception;
}
