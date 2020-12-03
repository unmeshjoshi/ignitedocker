package com.demobank.streaming.spark;

import com.demobank.events.TransactionEvent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

public class TransactionsStreamProcessor implements Serializable {

    private String brokerListenAddress;
    private String transactionsTopicName;

    public TransactionsStreamProcessor(String brokerListenAddress, String transactionsTopicName) {
        this.brokerListenAddress = brokerListenAddress;
        this.transactionsTopicName = transactionsTopicName;
    }

    public void writeTransactions() throws TimeoutException {
        var conf = new SparkConf().setAppName("TransactionsProcessor").setMaster("local");
        var spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> df
                = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokerListenAddress)
                .option("subscribe", transactionsTopicName)
                .option("kafka.metadata.max.age.ms", "1")
                .option("kafka.default.api.timeout.ms", "3000")
                .option("startingOffsets", "earliest") //Must for tests.
                .option("checkpointLocation", "/tmp/checkpoint_hbase") //Must for tests.
                .load();
        Dataset<Row> rowDataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        DataStreamWriter<Row> rowDataStreamWriter = rowDataset.writeStream();

        ForeachWriter<Row> writer = new IgniteWriter();
        rowDataStreamWriter.foreach(writer).start();
    }

    public void writeToCocroachDb() throws TimeoutException {
        var conf = new SparkConf().setAppName("TransactionsProcessor").setMaster("local");
        var spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> df
                = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokerListenAddress)
                .option("subscribe", transactionsTopicName)
                .option("kafka.metadata.max.age.ms", "1")
                .option("kafka.default.api.timeout.ms", "3000")
                .option("startingOffsets", "earliest") //Must for tests.
                .option("checkpointLocation", "/tmp/checkpoint_hbase") //Must for tests.
                .load();
        Dataset<Row> rowDataset = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        DataStreamWriter<Row> rowDataStreamWriter = rowDataset.writeStream();

        ForeachWriter<Row> writer = new CockroachDbWriter();
        rowDataStreamWriter.foreach(writer).start();
    }
}
