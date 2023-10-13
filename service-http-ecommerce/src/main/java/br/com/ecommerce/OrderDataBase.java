package br.com.ecommerce;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrderDataBase implements Closeable {

    private final LocalDataBase dataBase;

    public OrderDataBase() throws SQLException {
        this.dataBase = new LocalDataBase("orders_database");
        this.dataBase.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key)");
    }

    public boolean saveNew(Order order) throws SQLException {
        if (wasProcessed(order)) {
            return false;
        }
        dataBase.update("insert into Orders (uuid) values (?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = dataBase.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    @Override
    public void close() throws IOException {
        try {
            dataBase.close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
