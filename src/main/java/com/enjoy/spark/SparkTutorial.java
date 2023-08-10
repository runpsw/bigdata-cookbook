package com.enjoy.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author psw
 *
 * spark official tutorial of java api
 */
public class SparkTutorial {

    public static void main(String[] args) {

        JavaSparkContext sc = createDefualSc();

        // word count simple
//        wordCountSimple(sc);

        // read from local file system
        String path = "data/wc-data.txt";
        String pairPath = "data/pair.txt";
        JavaRDD<String> strRdd = sc.textFile(path);
        JavaPairRDD<String, String> pairRdd = sc.wholeTextFiles(pairPath);

        strRdd.collect().stream().forEach(System.out::println);
        System.out.println("--------------------------------");
        Map<String, String> map = pairRdd.collectAsMap();
        Set<String> strings = map.keySet();
        for (String string : strings) {
            System.out.println(string + "->" + map.get(string));
        }

        sc.close();
    }

    private static void wordCountSimple(JavaSparkContext sc) {
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

    @NotNull
    private static JavaSparkContext createDefualSc() {
        SparkConf conf = new SparkConf()
                .setAppName("spark core tutorial")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        return sc;
    }
}
