package com.enjoy.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @author psw
 * @date 2023/08/09
 *
 * spark official tutorial of java api
 */
public class SparkTutorial {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("spark core tutorial")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String path = "data/wc-data.txt";
        JavaRDD<String> rowRdd = sc.textFile(path);

        JavaRDD<String> wordsRdd = rowRdd.flatMap(str -> Arrays.stream(str.split(" ")).iterator());

        JavaPairRDD<String, Integer> word2one = wordsRdd.mapToPair(word -> new Tuple2<String, Integer>(word, 1));

        JavaPairRDD<String, Integer> wordCount = word2one.reduceByKey((a, b) -> a + b);

        // 普通的按照key进行升序排序
        JavaPairRDD<String, Integer> rs = wordCount.sortByKey(true);

        // 按照value 进行排序
        JavaPairRDD<Integer, String> sortedByValue = rs.mapToPair(tp -> new Tuple2<>(tp._2(), tp._1())).sortByKey(false);
        List<Tuple2<String, Integer>> collect = sortedByValue.mapToPair(tp -> new Tuple2<>(tp._2(), tp._1())).collect();

        System.out.println(collect);
    }
}
