package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;

import java.io.IOException;

public class TwitterHashtags {

    @SuppressWarnings("serial")
	public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        stream.foreachRDD(new VoidFunction<JavaRDD<Status>>() {
        	
			@Override
            public void call(JavaRDD<Status> rdd) throws Exception {
                rdd.foreach(new VoidFunction<Status>() {
					
					@Override
                    public void call(Status s) throws Exception {
                        DynamoHashTagRepository w = new DynamoHashTagRepository();
                        w.write(s);
                    }
                });
            }
        });
        
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
