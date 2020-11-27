package com.demobank.streaming.spark;

import com.demobank.cache.ignite.IgniteFactory;
import com.demobank.events.TransactionEvent;
import com.demobank.repository.KafkaTestUtils;
import com.demobank.repository.TestUtils;
import com.demobank.repository.TransactionRepository;
import com.thoughtworks.xstream.XStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.math.BigInteger;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TransactionsStreamProcessorTest {
    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("docker-compose.yml")).withLocalCompose(true);
                     //local mode is important.TODO:Figure out how to do dns resolution otherwise

    private static String transactionsTopicName = "parsedTxns";
    private static String brokerListenAddress = "localhost:9002";

    @BeforeClass
    public static void startKafka() {
        KafkaTestUtils.startEmbeddedKafka(brokerListenAddress);
    }

    @Test
    public void shouldStreamTransactionEventsToIgnite() throws InterruptedException, ExecutionException, TimeoutException {
        produceTransactionEvents("9952388700", 100);

        TransactionsStreamProcessor streamProcessor = new TransactionsStreamProcessor(brokerListenAddress, transactionsTopicName);
        streamProcessor.processTransactions();

        var ignite = new IgniteFactory().startOrGetIgniteInClientMode();
        TestUtils.waitUntilTrue(()->{
            return new TransactionRepository(ignite).findByAccount("9952388700").size() == 100;
        }, "waiting for transactions to be saved", Duration.ofSeconds(5));
    }

    private void produceTransactionEvents(String accountNumber, int noOfEvents) throws InterruptedException, ExecutionException {
        System.setProperty("hadoop.home.dir", "/");

        KafkaProducer<String, String> producer = KafkaTestUtils.createProducer(brokerListenAddress);
        List<TransactionEvent> transactionEvents = generateTransactionEvents(accountNumber, noOfEvents);
        XStream xStream = new XStream();
        for (TransactionEvent transactionEvent : transactionEvents) {
            var data = new ProducerRecord<String, String>(transactionsTopicName, transactionEvent.getTransactionId(), xStream.toXML(transactionEvent));
            var value = producer.send(data);
            value.get();//wait
        }
    }

    private long tranKey = 0l;
    private List<TransactionEvent> generateTransactionEvents(String accountNumber, int noOfEvents) {
        String transactionDate = "2020-02-02";
        List<TransactionEvent> transactions = new java.util.ArrayList<>();
        for (int i = 0; i < noOfEvents; i++) {
            UUID tranasctionId = UUID.randomUUID();
            BigInteger randomAmount = BigInteger.valueOf(new Random().nextInt(1000000));
            transactions.add(new TransactionEvent(tranasctionId.toString(), tranKey++, transactionDate, randomAmount, "Taxes", accountNumber));
        }
        return transactions;
    }

}