package com.roundyuan;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @program: SparkLearn
 * @description:
 * @author: Mr.Zhangmy
 * @create: 2020-09-06 19:09
 **/
public class JavaLandaWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("javaLandWordCount");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = jsc.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> counts = wordAndOne.reduceByKey((m, n) -> m + n);
        counts.saveAsTextFile(args[1]);
    }
}
