package com.ljy.sessionanalyze.sparkhive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class ReadHiveData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ReadHiveData")
                /*.setMaster("local[2]")*/;

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hsc = new HiveContext(sc.sc());
        hsc.sql("use testdb");
        DataFrame sql = hsc.sql("select * from spark_table");
        sql.show();
        sc.stop();
    }
}
