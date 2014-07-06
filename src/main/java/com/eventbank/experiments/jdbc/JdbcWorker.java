package com.eventbank.experiments.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcWorker {

    protected final String user;
    protected final String pass;
    protected final String url;
    protected Connection connection;
    private static final String driver = "com.mysql.jdbc.Driver";

    public JdbcWorker(String user, String pass, String url) {
        this.user = user;
        this.pass = pass;
        this.url = url;
        this.connection = newConnection();
    }

    static {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private Connection newConnection() {
        try {
            final Connection conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IllegalStateException();
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}