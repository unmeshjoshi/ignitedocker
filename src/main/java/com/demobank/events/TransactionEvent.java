package com.demobank.events;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.math.BigInteger;

public class TransactionEvent {
    private String transactionId;
    private Long tranKey;
    private String date;
    private BigInteger amount;
    private String type;
    private String accountNumber;

    public TransactionEvent(String transactionId, Long tranKey, String date, BigInteger amount, String type, String accountNumber) {
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

    public Long getTranKey() {
        return tranKey;
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

    public TransactionEvent() {
    }
}
