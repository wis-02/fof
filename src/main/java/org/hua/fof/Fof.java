package org.hua.fof;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Fof {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static JavaPairRDD<Long, Long> compute(JavaRDD<String> lines) {

        // TODO
        JavaPairRDD<Long, Long> edges = lines.mapToPair(s -> {
            String[] words = SPACE.split(s);
            return new Tuple2<>(Long.parseLong(words[0]), Long.parseLong(words[1]));
        });  //
        JavaPairRDD<Long, Long> reverse_edges = edges.mapToPair(t -> {
            return new Tuple2<>(t._2(), t._1());
        });
        JavaPairRDD<Long, Tuple2<Long, Long>> join_edges = reverse_edges.join(edges);
        JavaPairRDD<Long, Long> fof_edges = join_edges.mapToPair(t -> t._2());
        return fof_edges;
    }

    public static void main(String[] args) throws Exception {
/*
        if (args.length < 2) {
            System.err.println("Usage: Fof <input-path> <output-path>");
            System.exit(1);
        }*/

        SparkConf sparkConf = new SparkConf().setAppName("Fof").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\USER\\Desktop\\lab-spark\\data\\example2\\input\\out.ego-facebook");

        JavaPairRDD<Long, Long> fofEdges = compute(lines);

        fofEdges.saveAsTextFile("C:\\Users\\USER\\Desktop\\lab-spark\\data\\example2\\output");

        sc.stop();

    }
}