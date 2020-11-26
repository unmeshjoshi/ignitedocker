package com.demobank.repository;

import com.demobank.events.TransactionEvent;
import com.demobank.models.Transaction;
import com.thoughtworks.xstream.XStream;
import net.manub.embeddedkafka.EmbeddedKafka;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import scala.Predef;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TransactionRepositoryTest implements Serializable {

    @Test
    public void kafkaStreaming() throws InterruptedException, ExecutionException, TimeoutException {
        System.setProperty("hadoop.home.dir", "/");

        EmbeddedKafka.start(createKafkaConfig());
        Thread.sleep(1000);
        System.out.println("Started Kafka");
        KafkaProducer<String, String> producer = createProducer();
        List<TransactionEvent> transactionEvents = generateTransactionEvents("9952388700");
        XStream xStream = new XStream();
        for (TransactionEvent transactionEvent : transactionEvents) {
            var data = new ProducerRecord<String, String>("parsedTxns", transactionEvent.getTransactionId(), xStream.toXML(transactionEvent));
            var value = producer.send(data);
            value.get();//wait
        }

        var conf = new SparkConf().setAppName("StructuredKafkaProcessing").setMaster("local");
        var spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> df
                = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9002")
                .option("subscribe", "parsedTxns")
                .option("kafka.metadata.max.age.ms", "1")
                .option("kafka.default.api.timeout.ms", "3000")
                .option("startingOffsets", "earliest") //Must for tests.
                .option("checkpointLocation", "/tmp/checkpoint_hbase") //Must for tests.
                .load();
        Dataset<Row> rowDataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");

        DataStreamWriter<Row> rowDataStreamWriter = rowDataset.writeStream();
        ForeachWriter<Row> writer = new ForeachWriter<Row>() {

            @Override
            public boolean open(long partitionId, long epochId) {
                return true;
            }

            @Override
            public void process(Row row) {
                var xmlMessage = row.getString(1);
                XStream xStream1 = new XStream();
                TransactionEvent transaction = (TransactionEvent) xStream1.fromXML(xmlMessage);
                System.out.println("Transaction writing " + transaction);
            }

            @Override
            public void close(Throwable errorOrNull) {

            }
        };
        rowDataStreamWriter.foreach(writer).start();


        Thread.sleep(10000);
    }


    private KafkaProducer<String, String> createProducer() {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9002");
        props.put("application.id", UUID.randomUUID().toString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        var producer = new KafkaProducer<String, String>(props);
        return producer;
    }


    private EmbeddedKafkaConfig createKafkaConfig() {
        var kafkaHost = "localhost";
        var kafkaPort = 9002;
        var brokers = "PLAINTEXT://" + kafkaHost + ":" + kafkaPort;
        HashMap<String, String> config = new HashMap<String, String>();
        config.put("listeners", brokers);
        config.put("advertised.listeners", brokers);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        EmbeddedKafkaConfig apply = EmbeddedKafkaConfig.apply(6000,
                6001,
                toScalaMap(config),
                toScalaMap(new HashMap<String, String>()),
                toScalaMap(new HashMap<String, String>()));
        return apply;
    }

    public static <A, B> Map<A, B> toScalaMap(java.util.Map<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
                Predef.<Tuple2<A, B>>conforms()
        );
    }

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


    private List<TransactionEvent> generateTransactionEvents(String accountNumber) {
        String transactionDate = "2020-02-02";
        List<TransactionEvent> transactions = new java.util.ArrayList<>();
        for (int i = 0; i < 100; i++) {
            UUID tranasctionId = UUID.randomUUID();
            BigInteger randomAmount = BigInteger.valueOf(new Random().nextInt(1000000));
            transactions.add(new TransactionEvent(tranasctionId.toString(), tranKey++, transactionDate, randomAmount, "Taxes", accountNumber));
        }
        return transactions;
    }

}