#!/bin/sh

tmp=$(mktemp)

echo "Preprocessing..."
sed '1d' crypto-markets.csv | sort -t ',' -k4 | awk 'BEGIN {FS=","} {print $4 ";" $3 ";" $9}' > $tmp

for year in {2013..2018}
do
    echo "Year $year processing..."
    grep "^$year" $tmp | cut -c 12- > crypto-$year.csv
done

rm $tmp

# If authenticated then upload to my bucket
gsutil ls 2>/dev/null | grep soslanco > /dev/null
if [ $? -eq 0 ]; then
    for year in {2013..2018}
    do
        echo
        gsutil cp crypto-$year.csv gs://soslanco
    done
    gsutil ls gs://soslanco | sed 's!gs://!https://storage.googleapis.com/!' > URLs.txt
fi
