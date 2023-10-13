package br.com.ecommerce;

import java.sql.*;

public class LocalDataBase {
    private final Connection connection;

    public LocalDataBase(String name) throws SQLException {
        String url = "jdbc:sqlite:" + name + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public ResultSet query(String query, String... params) throws SQLException {
        return getPreparedStatement(query, params).executeQuery();
    }

    public boolean update(String statement, String... params) throws SQLException {
        return getPreparedStatement(statement, params).execute();
    }

    private PreparedStatement getPreparedStatement(String statement, String[] params) throws SQLException {
        var prepareStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            prepareStatement.setString(i + 1, params[i]);
        }
        return prepareStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
