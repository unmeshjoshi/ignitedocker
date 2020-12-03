package com.demobank.streaming.spark;

import com.demobank.cache.ignite.IgniteFactory;
import com.demobank.events.TransactionEvent;
import com.demobank.models.Transaction;
import com.demobank.repository.TransactionCockroachDbRepository;
import com.demobank.repository.TransactionIgniteRepository;
import com.thoughtworks.xstream.XStream;
import org.apache.ignite.Ignite;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

public class CockroachDbWriter extends ForeachWriter<Row> {
    @Override
    public boolean open(long partitionId, long epochId) {
        return true;
    }

    @Override
    public void process(Row row) {
        var xmlMessage = row.getString(1);
        XStream xStream = new XStream();
        TransactionEvent event = (TransactionEvent) xStream.fromXML(xmlMessage);
        TransactionCockroachDbRepository transactionIgniteRepository = new TransactionCockroachDbRepository();
        transactionIgniteRepository.save(new Transaction(event.getTransactionId(),
                event.getTranKey(), event.getDate(),
                event.getAmount(), event.getType(), event.getAccountNumber()));
    }

    @Override
    public void close(Throwable errorOrNull) {

    }
}
