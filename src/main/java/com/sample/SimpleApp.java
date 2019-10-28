package com.sample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 */
public class SimpleApp {

    public static void main(String[] args) {
        String logFile = "../spark-2.4.4-bin-hadoop2.7/README.md";
        SparkSession spark = SparkSession.builder()
                .appName("Simple Application")
                // https://blog.csdn.net/shenlanzifa/article/details/42679577
                // 本地单线程
//                .master("local")
                // 本地多线程（指定K个内核）
//                .master("local[*]")
                // 本地多线程（指定所有可用内核）
                .master("local[*]")
                // 连接到指定的 Spark standalone cluster master，需要指定端口。http://spark.apache.org/docs/latest/spark-standalone.html
//                .master("spark://HOST:PORT")
                // mesos://HOST:PORT 连接到指定的 Mesos 集群，需要指定端口。http://spark.apache.org/docs/latest/running-on-mesos.html
//                .master("mesos://HOST:PORT")
                // yarn-client客户端模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。
                // yarn-cluster集群模式 连接到 YARN 集群。需要配置 HADOOP_CONF_DIR。
                // http://spark.apache.org/docs/latest/running-on-yarn.html
                .getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();
    }
}
