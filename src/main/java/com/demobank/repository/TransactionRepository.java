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
import java.util.List;
import java.util.stream.Collectors;

public class TransactionRepository {

    private final Ignite ignite;

    public TransactionRepository(Ignite ignite) {
        this.ignite = ignite;
    }

    public CacheConfiguration<TransactionKey, Transaction> getCacheConfiguration() {
        CacheConfiguration<TransactionKey, Transaction> transactionCacheConfig = new CacheConfiguration<>("transactionCache");
        transactionCacheConfig.setCacheMode(CacheMode.PARTITIONED);
        transactionCacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        transactionCacheConfig.setBackups(1);
        transactionCacheConfig.setIndexedTypes(TransactionKey.class, Transaction.class);

        return transactionCacheConfig;
    }

    public void save(Transaction transaction) {
      IgniteCache<TransactionKey, Transaction> cache = getOrCreateCache();
      cache.putIfAbsent(new TransactionKey(transaction.getTransactionId()), transaction);
    }

    public IgniteCache<TransactionKey, Transaction> getOrCreateCache() {
        return ignite.getOrCreateCache(getCacheConfiguration());
    }

    public List<Transaction> findByAccount(String accountNumber) {
        String sql = "select * from Transaction where accountNumber = ?" ;

        IgniteCache<TransactionKey, Transaction> cache = getOrCreateCache();
        SqlFieldsQuery query = new SqlFieldsQuery(sql, true)
                               .setArgs(accountNumber)
                               .setPageSize(25); //this is not for pagination.

        try (FieldsQueryCursor<List<?>> cursor = cache.query(query)) {
            List<List<?>> all = cursor.getAll();
            //TODO: Use RowMapper?
            return all.stream().map(list -> {
                return new Transaction((String)list.get(0),
                            (Long) list.get(1),
                        (String)list.get(2),
                        (BigInteger)list.get(3),
                        (String)list.get(4),
                        (String)list.get(5));
            }).collect(Collectors.toList());
        }
    }

    public void printStats() {
        IgniteCache<TransactionKey, Transaction> cache = getOrCreateCache();

        System.out.println("Transactions cache size (Local): " + cache.localSize());
        System.out.println("Transactions cache size (Primary): " + cache.size(CachePeekMode.PRIMARY));
        System.out.println("Transactions cache size (Backup): " + cache.size(CachePeekMode.BACKUP));
    }
}