package com.ljy.sessionanalyze.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

public class Test {
    public static void main(String[] args){
        List<Integer>list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        final SparkConf conf = new SparkConf().setAppName("Test");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<Integer> rdd = sc.parallelize(list);
        final JavaRDD<Integer> repRDD = rdd.repartition(5);
        int mat = repRDD.sortBy(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer;
            }
        },false,5).collect().get(0);
        System.out.println(mat);
        sc.stop();
    }
}
