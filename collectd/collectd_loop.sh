#!/bin/bash

cd /opt/mongodb-tools
. virtualenv/bin/activate
generation=1000
export COLLECTD_INTERVAL
export COLLECTD_HOSTNAME
cache_file=/var/lib/t2s/mongo_index_stats_cache.txt

while [ $generation -gt 0 ]
do
   if [ ! -f $cache_file -o "`find $cache_file -mmin +30`" != "" ]
   then 
     ./mongodbtools/index_stats_collectd.py > $cache_file
   fi

   cat $cache_file

   sleep $COLLECTD_INTERVAL

   generation=$(expr $generation - 1)
done
