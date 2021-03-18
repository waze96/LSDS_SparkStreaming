# BENCHMARKS 

## GitHub Repo: https://github.com/waze96/LSDS_SparkStreaming

#### NOTES:
Commands to run the applications:

###### TwitterStreamingExample

./spark-submit --class upf.edu.TwitterStreamingExample --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/waze/eclipse-workspace/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT-fat.jar /home/waze/eclipse-workspace/lab3/src/main/resources/application.properties

###### TwitterStateless

./spark-submit --class upf.edu.TwitterStateless --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/waze/eclipse-workspace/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT-fat.jar /home/waze/eclipse-workspace/lab3/src/main/resources/application.properties /home/waze/eclipse-workspace/lab3/src/main/resources/map.tsv

###### TwitterWithWindow

./spark-submit --class upf.edu.TwitterWithWindow --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/waze/eclipse-workspace/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT-fat.jar /home/waze/eclipse-workspace/lab3/src/main/resources/application.properties /home/waze/eclipse-workspace/lab3/src/main/resources/map.tsv

###### TwitterWithState

./spark-submit --class upf.edu.TwitterWithState --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/waze/eclipse-workspace/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT-fat.jar /home/waze/eclipse-workspace/lab3/src/main/resources/application.properties es

###### TwitterHashtags

./spark-submit --class upf.edu.TwitterHashtags --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/waze/eclipse-workspace/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT-fat.jar /home/waze/eclipse-workspace/lab3/src/main/resources/application.properties 

###### TwitterHashtagsReader

./spark-submit --class upf.edu.TwitterHashtagsReader --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/waze/eclipse-workspace/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT-fat.jar es
