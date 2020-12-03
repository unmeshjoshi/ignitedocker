package com.demobank.repository;

import java.math.BigInteger;
import java.sql.*;
import java.util.Random;
import java.util.UUID;

public class CockroachExampleApp {
    public static void main(String[]args) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:26257/defaultdb", "root", "admin")) {
            System.out.println("Java JDBC PostgreSQL Example");
            System.out.println("Connected to CockroachDB database!");
            Statement statement = connection.createStatement();

            PreparedStatement pt = connection.prepareStatement("CREATE TABLE Transactions (transactionId STRING PRIMARY KEY, tranKey STRING, tranDate STRING, amount DECIMAL, tranType STRING);");
            pt.executeUpdate();

            long tranKey = 0l;
            for (int i = 0;i < 1000; i++) {
                UUID tranasctionId = UUID.randomUUID();
                BigInteger randomAmount = BigInteger.valueOf(new Random().nextInt(1000000));
                PreparedStatement p = connection.prepareStatement("insert into transactions(transactionId, tranKey, tranDate, amount, tranType) values(?, ?, ?, ?, ?)");
                p.setString(1, tranasctionId.toString());
                p.setString(2, tranKey++ + "");
                p.setString(3, "2020-02-02");
                p.setLong(4, randomAmount.longValue());
                p.setString(5, "Credit");
                p.executeUpdate();
            }


            ResultSet resultSet = statement.executeQuery("SELECT * FROM public.\"transactions\";");
            while (resultSet.next()) {
                System.out.printf("%-30.30s  %-30.30s%n", resultSet.getString("tranType"), resultSet.getLong("amount"));
            }

        } /*catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC driver not found.");
            e.printStackTrace();
        }*/ catch (SQLException e) {
            System.out.println("Connection failure.");
            e.printStackTrace();
        }
    }
}
