#!/bin/bash


/usr/local/hadoop/bin/hadoop jar /usr/local/hadoop/contrib/streaming/hadoop-streaming-1.2.1.jar -input /user/hduser/csv_data/apat63_99.txt -output output_streaming -file   AttributeCount.py -mapper 'AttributeCount.py 1' -reducer aggregate
