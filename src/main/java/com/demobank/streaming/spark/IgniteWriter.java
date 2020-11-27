package com.demobank.streaming.spark;

import com.demobank.cache.ignite.IgniteFactory;
import com.demobank.events.TransactionEvent;
import com.demobank.models.Transaction;
import com.demobank.repository.TransactionRepository;
import com.thoughtworks.xstream.XStream;
import org.apache.ignite.Ignite;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;

public class IgniteWriter extends ForeachWriter<Row> {

    public IgniteWriter() {
    }

    @Override
    public boolean open(long partitionId, long epochId) {
        return true;
    }

    @Override
    public void process(Row row) {
        Ignite ignite = new IgniteFactory().startOrGetIgniteInClientMode();
        var xmlMessage = row.getString(1);
        XStream xStream = new XStream();
        TransactionEvent event = (TransactionEvent) xStream.fromXML(xmlMessage);
        TransactionRepository transactionRepository = new TransactionRepository(ignite);
        transactionRepository.save(new Transaction(event.getTransactionId(),
                event.getTranKey(), event.getDate(),
                event.getAmount(), event.getType(), event.getAccountNumber()));
    }

    @Override
    public void close(Throwable errorOrNull) {

    }
}
