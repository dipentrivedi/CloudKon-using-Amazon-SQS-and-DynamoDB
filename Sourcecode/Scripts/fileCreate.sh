#!/bin/bash

mkdir 1worker
cd 1worker

for((i=0; i<1000; i++))
do
	echo "sleep 10" >> 10
done

for((i=0; i<100; i++))
do
	echo "sleep 1000" >> 1000
done

for((i=0; i<10; i++))
do
	echo "sleep 10000" >> 10000
done

cd ..

mkdir 2worker

cd 2worker

for((i=0; i<2000; i++))
do
	echo "sleep 10" >> 10
done

for((i=0; i<200; i++))
do
	echo "sleep 1000" >> 1000
done

for((i=0; i<20; i++))
do
	echo "sleep 10000" >> 10000
done

cd ..

mkdir 4worker

cd 4worker

for((i=0; i<4000; i++))
do
	echo "sleep 10" >> 10
done

for((i=0; i<400; i++))
do
	echo "sleep 1000" >> 1000
done

for((i=0; i<40; i++))
do
	echo "sleep 10000" >> 10000
done

cd ..

mkdir 8worker

cd 8worker

for((i=0; i<8000; i++))
do
	echo "sleep 10" >> 10
done

for((i=0; i<800; i++))
do
	echo "sleep 1000" >> 1000
done

for((i=0; i<80; i++))
do
	echo "sleep 10000" >> 10000
done

cd ..

mkdir 16worker

cd 16worker

for((i=0; i<16000; i++))
do
	echo "sleep 10" >> 10
done

for((i=0; i<1600; i++))
do
	echo "sleep 1000" >> 1000
done

for((i=0; i<160; i++))
do
	echo "sleep 10000" >> 10000
done

cd ..

