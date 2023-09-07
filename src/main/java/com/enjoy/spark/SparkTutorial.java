package com.enjoy.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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

        // 理解cogroup 算子的功能, 按照key 将相同的key的value组织到一个tuple中，key还是key，value 编程了iterable了
//        cogroupDemo(sc);

        // aggregateByKey 算子功能：对KV使用给定的（i1,i2）进行聚合计算，当给定（0[agg 初始值]，0[count 的初始值]）的时候，返回(k,(agg, count-key))
        aggregateByKeyDemo(sc);

        sc.close();
    }

    private static void aggregateByKeyDemo(JavaSparkContext sc) {
        JavaRDD<String> rawRdd = sc.textFile("data/pair.txt");
        JavaRDD<Tuple2<String, Integer>> tpRdd = rawRdd.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                String[] arr = s.split(" ");
                ArrayList<Tuple2<String, Integer>> lst = new ArrayList<>();
                lst.add(new Tuple2<>(arr[0], Integer.parseInt(arr[1])));
                return lst.iterator();
            }
        });

        JavaPairRDD<String, Integer> pairRDD = JavaPairRDD.fromJavaRDD(tpRdd);
        JavaPairRDD<String, Tuple2<Integer, Integer>> aggByKeyPairRdd = pairRDD.aggregateByKey(new Tuple2<>(0, 0), new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> acc1, Integer value) throws Exception {
                return new Tuple2<>(acc1._1() + value, acc1._2() + 1);
            }
        }, new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> tp1, Tuple2<Integer, Integer> tp2) throws Exception {
                return new Tuple2<>(tp1._1() + tp2._1(), tp1._2() + tp2._2());
            }
        });

        JavaPairRDD<String, Double> avgV = aggByKeyPairRdd.mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
            @Override
            public Double call(Tuple2<Integer, Integer> tp) throws Exception {
                return tp._1() * 1.0 / tp._2();
            }
        });

        System.out.println(avgV.take(3));
    }

    private static void cogroupDemo(JavaSparkContext sc) {
        JavaRDD<String> strRdd = sc.textFile("data/pair.txt");
        JavaPairRDD<String, String> rdd1 = strRdd.mapToPair(line -> new Tuple2(line.split(" ")[0], line.split(" ")[1]));

        JavaPairRDD<String, String> rdd2 = rdd1.mapToPair(tp -> new Tuple2<>(tp._1(), tp._2() + 1));

        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroup = rdd1.cogroup(rdd2);
        System.out.println(cogroup.collect());
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
