package com.demobank.models;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.math.BigInteger;
//CREATE TABLE Transactions (transactionId STRING PRIMARY KEY, tranKey STRING, tranDate STRING, amount DECIMAL, tranType STRING);
public class Transaction {
    @QuerySqlField
    private String transactionId;
    @QuerySqlField
    private Long tranKey;
    @QuerySqlField
    private String date;
    @QuerySqlField
    private BigInteger amount;
    @QuerySqlField
    private String type;

    @QuerySqlField(index=true)
    private String accountNumber;

    public Transaction() {
    }

    public Transaction(String transactionId, Long tranKey, String date, BigInteger amount, String type, String accountNumber) {
        this.transactionId = transactionId;
        this.tranKey = tranKey;
        this.date = date;
        this.amount = amount;
        this.type = type;
        this.accountNumber = accountNumber;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getDate() {
        return date;
    }

    public BigInteger getAmount() {
        return amount;
    }

    public String getType() {
        return type;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public Long getTranKey() {
        return tranKey;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId='" + transactionId + '\'' +
                ", tranKey=" + tranKey +
                ", date='" + date + '\'' +
                ", amount=" + amount +
                ", type='" + type + '\'' +
                ", accountNumber='" + accountNumber + '\'' +
                '}';
    }
}