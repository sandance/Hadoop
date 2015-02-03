javac -classpath /usr/local/hadoop/hadoop-core-1.2.1.jar:/usr/local/hadoop/lib/commons-cli-1.2.jar  -d playground/classes playground/src/WordCount.java
jar -cvf playground/wordcount.jar -C playground/classes/ .
/usr/local/hadoop/bin/hadoop jar playground/wordcount.jar org.apache.hadoop.examples.WordCount /user/hduser/gutenberg output



/usr/local/hadoop/bin/hadoop fs -cat  /user/hduser/output/part-r-00000
