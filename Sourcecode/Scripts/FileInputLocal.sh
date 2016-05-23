#!/bin/bash

javac LocalSQSProg.java

mkdir Outputs
mkdir Outputs/LocalSQS

for thread in 8 
	do
	for time in 10 1000 10000
		do
		echo "Operation Performing for time "$time" ms, Thread : "$thread
		java LocalSQSProg $thread $thread"worker"/$time >> ClientOutput"_"$thread"worker""_"$time.txt
		
		mv ClientOutput"_"$thread"worker""_"$time.txt Outputs/LocalSQS	
	done
done

