package com.roundyuan;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @program: SparkLearn
 * @description:
 * @author: Mr.Zhangmy
 * @create: 2020-09-06 18:21
 **/
public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("javaWordCount");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = jsc.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                Iterator<String> iterator = Arrays.asList(s.split(" ")).iterator();
                return iterator;
            }
        });
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> wordAndNum = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairRDD<Integer, String> valueAndKey = wordAndNum.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.swap();
            }
        });

        JavaPairRDD<Integer, String> sorted = valueAndKey.sortByKey();
        JavaPairRDD<String, Integer> res = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });
        res.saveAsTextFile(args[1]);
    }
}
