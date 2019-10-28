package com.sample;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

/**
 * Hello world!
 */
public class SimpleApp {

    public static void main(String[] args) {
        String logFile = "../spark-2.4.4-bin-hadoop2.7/README.md";
        SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[*]").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
