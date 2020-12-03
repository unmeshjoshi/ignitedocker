package com.demobank.repository;

import com.demobank.models.Transaction;
import com.demobank.models.TransactionKey;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TransactionCockroachDbRepository {

    public void save(Transaction transaction) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:26257/defaultdb", "root", "admin")) {
            PreparedStatement p = connection.prepareStatement("insert into transactions(transactionId, tranKey, tranDate, amount, accountnumber, tranType) values(?, ?, ?, ?, ?, ?)");
            p.setString(1, transaction.getTransactionId());
            p.setString(2, transaction.getTranKey()+"");
            p.setString(3, transaction.getDate());
            p.setLong(4, transaction.getAmount().longValue());
            p.setString(5, transaction.getAccountNumber());
            p.setString(6, transaction.getType());
            p.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public List<Transaction> findByAccount(String accountNumber) {
        String sql = "select * from Transaction where accountNumber = ?";
        List<Transaction> result = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:26257/defaultdb", "root", "admin")) {
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM public.\"transactions\";");
            while (rs.next()) {
                result.add(new Transaction(rs.getString("transactionId"),
                        Long.parseLong(rs.getString("tranKey")),
                        rs.getString("tranDate"),
                        new BigInteger(String.valueOf(rs.getLong("amount"))),
                        rs.getString("tranType"),
                        rs.getString("accountNumber")));

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}