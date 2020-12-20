#!/bin/bash

latestAvailableDir1=$(hadoop fs -ls $1 | awk '{print $8}' | sort -ru | head -1 )
latestAvailableDir2=$(hadoop fs -ls $2 | awk '{print $8}' | sort -ru | head -1 )

echo "latestAvailableDir1="$latestAvailableDir1;
echo "latestAvailableDir2="$latestAvailableDir2;
