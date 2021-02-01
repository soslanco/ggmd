#!/bin/sh

cd client

rm ./client

go build


if [ -f ./client ]; then

	# Fetch
	for year in {2018..2013} # backward order for test purpose
	do
		echo "Year $year"
		./client -fetch "https://storage.googleapis.com/soslanco/crypto-$year.csv"
		sleep 3 # slow down for time difference in timestamps
	done

	# List
	./client -lid 0 -lsize 10 -lsort "name"
	./client -lid 10 -lsize 10 -lsort "name"


	./client -lid 0 -lsize 10 -lsort "price" -lsortdir -1
	./client -lid 10 -lsize 10 -lsort "price" -lsortdir -1
fi
