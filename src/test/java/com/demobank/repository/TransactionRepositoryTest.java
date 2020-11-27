package com.demobank.repository;

import com.demobank.cache.ignite.IgniteFactory;
import com.demobank.models.Transaction;
import org.apache.ignite.Ignite;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Ignore //Run docker-compose up before running this test. For debugging.
public class TransactionRepositoryTest {
    @Test
    public void setAndQueryTransactions() {
        Ignite ignite = new IgniteFactory().startOrGetIgniteInClientMode();

        String accountNumber = "9952388700";
        //With dockerized ignite setup, seed adds transactions every time. FIXME:
        generateTransactions(accountNumber);

        TransactionRepository transactionRepository = new TransactionRepository(new IgniteFactory().startOrGetIgniteInClientMode());

        List<Transaction> fetchedTransactions = transactionRepository.findByAccount(accountNumber);

        List<Transaction> filteredTxns = fetchedTransactions.stream().filter(t -> t.getAccountNumber().equals(accountNumber)).collect(Collectors.toList());
        //all transactions should have same account number
        assertEquals(fetchedTransactions.size(), filteredTxns.size());
    }

    private long tranKey = 0l;

    private void generateTransactions(String accountNumber) {
        TransactionRepository transactionRepository = new TransactionRepository(new IgniteFactory().startOrGetIgniteInClientMode());
        String transactionDate = "2020-02-02";

        for (int i = 0; i < 100; i++) {
            UUID tranasctionId = UUID.randomUUID();
            BigInteger randomAmount = BigInteger.valueOf(new Random().nextInt(1000000));
            transactionRepository.save(new Transaction(tranasctionId.toString(), tranKey++, transactionDate, randomAmount, "Taxes", accountNumber));
        }
        transactionRepository.printStats();

    }
}