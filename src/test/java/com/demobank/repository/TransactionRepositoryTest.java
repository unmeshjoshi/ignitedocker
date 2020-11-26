package com.demobank.repository;

import com.demobank.models.Transaction;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TransactionRepositoryTest {

    @Test
    public void readWriteTransactions() {
        Ignite ignite = startIgniteInClientMode();

        String accountNumber = "9952388700";
        //With dockerized ignite setup, seed adds transactions every time. FIXME:
        TransactionRepository transactionRepository = seedTransactionsForAccount(ignite, accountNumber);

        List<Transaction> fetchedTransactions = transactionRepository.findByAccount(accountNumber);

        List<Transaction> filteredTxns = fetchedTransactions.stream().filter(t -> t.getAccountNumber().equals(accountNumber)).collect(Collectors.toList());
        //all transactions should have same account number
        assertEquals(fetchedTransactions.size(), filteredTxns.size());
    }

    @NotNull
    private TransactionRepository seedTransactionsForAccount(Ignite ignite, String accountNumber) {
        TransactionRepository transactionRepository = new TransactionRepository(ignite);
        List<Transaction> transactions = generateTransactions(accountNumber);
        for (Transaction transaction : transactions) {
            transactionRepository.save(transaction);
        }
        transactionRepository.printStats();
        return transactionRepository;
    }

    private Ignite startIgniteInClientMode() {
        // Start Ignite in client mode.
        Ignition.setClientMode(true);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("ignite-01:47500..47509", "ignite-02:47500..47509", "ignite-03:47500..47509"));
        discoSpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoSpi);
        Ignite ignite = Ignition.start(cfg);
        return ignite;
    }

    private long tranKey = 0l;
    private List<Transaction> generateTransactions(String accountNumber) {
        String transactionDate = "2020-02-02";
        List<Transaction> transactions = new java.util.ArrayList<Transaction>();
        for (int i = 0; i < 100; i++) {
            UUID tranasctionId = UUID.randomUUID();
            BigInteger randomAmount = BigInteger.valueOf(new Random().nextInt(1000000));
            transactions.add(new Transaction(tranasctionId.toString(), tranKey++, transactionDate, randomAmount, "Taxes", accountNumber));
        }
        return transactions;
    }

}