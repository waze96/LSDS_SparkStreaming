package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;

public class TwitterWithWindow {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter with windows");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file as RDD
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);

        // create an initial stream that counts language within the batch (as in the previous exercise)
        final JavaPairDStream<String, Integer> languageCountStream = stream
        		.transformToPair(aux ->   aux.mapToPair(word -> new Tuple2 <> (word.getLang(),1))
                .join(languageMap)
                .mapToPair(x -> new Tuple2 <> (x._2._2,x._2._1)));

        // Prepare output within the batch
        final JavaPairDStream<Integer, String> languageBatchByCount = languageCountStream
        		.reduceByKey((a,b) -> a+b)						// Reduce by key (Language) to obtain the Sum of each Language.
        		.mapToPair(Tuple2::swap)						// Swap <String, Integer> (Language, Sum) to <Integer, String> (Sum, Language)
        		.transformToPair(s ->s.sortByKey(false));		// Sort by key (Sum) in descendent way to get the top 15.


        // Prepare output within the window
        final JavaPairDStream<Integer, String> languageWindowByCount = languageCountStream
        		.window(Durations.seconds(5*60))
        		.reduceByKey((a,b) -> a+b)
        		.mapToPair(Tuple2::swap)
        		.transformToPair(s ->s.sortByKey(false));

        // Print first 15 results for each one
        languageBatchByCount.print(15);
        languageWindowByCount.print(15);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTerminationOrTimeout(5*60000);
        jsc.stop();
    }
}
