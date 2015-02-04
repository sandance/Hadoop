javac -classpath hadoop-core-1.0.3.jar:lib/commons-cli-1.2.jar -d /home/hduser/hadoop_programs/bigram_frequency/classes /home/hduser/hadoop_programs/bigram_frequency/src/B*.java





jar -cvf /home/hduser/hadoop_programs/bigram_frequency/bigram_frequency.jar -C /home/hduser/hadoop_programs/bigram_frequency/classes/ .



bin/hadoop jar /home/hduser/hadoop_programs/bigram_frequency/bigram_frequency.jar org.rashed.hadoop.examples.BigramFrequency /user/hduser/test_data /user/hduser/bigram_freq_test

