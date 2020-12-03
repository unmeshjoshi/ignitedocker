package com.demobank.repository;

import com.demobank.cache.ignite.IgniteFactory;
import com.demobank.models.Transaction;
import org.apache.ignite.Ignite;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Ignore //Run docker-compose up before running this test. For debugging.
public class TransactionIgniteRepositoryTest {
    @Test
    public void setAndQueryTransactions() {
        String accountNumber = "9952388700";
        //With dockerized ignite setup, seed adds transactions every time. FIXME:
        generateTransactions(accountNumber);

        TransactionIgniteRepository transactionIgniteRepository = new TransactionIgniteRepository(new IgniteFactory().startOrGetIgniteInClientMode());

        List<Transaction> fetchedTransactions = transactionIgniteRepository.findByAccount(accountNumber);

        List<Transaction> filteredTxns = fetchedTransactions.stream().filter(t -> t.getAccountNumber().equals(accountNumber)).collect(Collectors.toList());
        //all transactions should have same account number
        assertEquals(fetchedTransactions.size(), filteredTxns.size());
    }

    private long tranKey = 0l;

    private void generateTransactions(String accountNumber) {
        TransactionIgniteRepository transactionIgniteRepository = new TransactionIgniteRepository(new IgniteFactory().startOrGetIgniteInClientMode());
        String transactionDate = "2020-02-02";
        Ignite ignite = new IgniteFactory().startOrGetIgniteInClientMode();
        org.apache.ignite.transactions.Transaction transaction = ignite.transactions().txStart();
        for (int i = 0; i < 100; i++) {
            UUID tranasctionId = UUID.randomUUID();
            BigInteger randomAmount = BigInteger.valueOf(new Random().nextInt(1000000));
            transactionIgniteRepository.save(new Transaction(tranasctionId.toString(), tranKey++, transactionDate, randomAmount, "Taxes", accountNumber));
        }
        transaction.commit();
        transactionIgniteRepository.printStats();

    }
}