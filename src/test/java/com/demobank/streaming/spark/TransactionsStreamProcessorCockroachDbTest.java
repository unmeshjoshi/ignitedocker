package com.demobank.streaming.spark;

import com.demobank.cache.ignite.IgniteFactory;
import com.demobank.events.TransactionEvent;
import com.demobank.repository.KafkaTestUtils;
import com.demobank.repository.TestUtils;
import com.demobank.repository.TransactionCockroachDbRepository;
import com.demobank.repository.TransactionIgniteRepository;
import com.thoughtworks.xstream.XStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class TransactionsStreamProcessorCockroachDbTest {
    @ClassRule
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("docker-compose-cocroachdb.yml")).withLocalCompose(true);
                     //local mode is important.TODO:Figure out how to do dns resolution otherwise

    private static String transactionsTopicName = "parsedTxns";
    private static String brokerListenAddress = "localhost:9002";

    @BeforeClass
    public static void setUp() throws SQLException {
        KafkaTestUtils.startEmbeddedKafka(brokerListenAddress);
        runMigrations();
    }

    //create transactions table
    private static void runMigrations() throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:26257/defaultdb", "root", "admin");
        PreparedStatement pt = connection.prepareStatement("CREATE TABLE Transactions (transactionId STRING PRIMARY KEY, tranKey STRING, tranDate STRING, amount DECIMAL, accountnumber STRING, tranType STRING);");
        pt.executeUpdate();
    }

    @Test
    public void shouldStreamTransactionEventsToCocroachDb() throws InterruptedException, ExecutionException, TimeoutException {
        produceTransactionEvents("9952388700", 100);

        TransactionsStreamProcessor streamProcessor = new TransactionsStreamProcessor(brokerListenAddress, transactionsTopicName);
        streamProcessor.writeToCocroachDb();

        var cockroachDbRepository = new TransactionCockroachDbRepository();
        TestUtils.waitUntilTrue(()->{
            return cockroachDbRepository.findByAccount("9952388700").size() == 100;
        }, "waiting for transactions to be saved", Duration.ofSeconds(10));
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