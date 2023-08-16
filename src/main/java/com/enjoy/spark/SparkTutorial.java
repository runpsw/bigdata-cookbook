package com.enjoy.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.jetbrains.annotations.NotNull;
import scala.Tuple2;

import java.util.*;

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

        /*
         read from local file system
         textFile(): 将文件内容读出来。可以给目录，文件，通配符文件
         wholeTextFiles(): 读出来的是pair rdd, key 是文件名，value 是文件内容，适用于读取小文件
         */
//        textFileVsWholeTextFiles(sc);

        // 并行化集合的方式创建RDD
//        paralleRdd(sc);

        // 理解cogroup 算子的功能

        sc.close();
    }

    private static void paralleRdd(JavaSparkContext sc) {
        List<Integer> lst = new ArrayList<>();
        lst.add(1);
        lst.add(2);
        JavaRDD<Integer> rdd = sc.parallelize(lst);
        rdd.collect();
    }

    private static void textFileVsWholeTextFiles(JavaSparkContext sc) {
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
