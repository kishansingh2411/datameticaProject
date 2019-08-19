#!/bin/bash

year_id=$1
month_id=$2
year_month_id=$3

echo "get the total count for the fact table"
fact_total=$(hive -e "SELECT count(*) FROM processed.f_vod_orders_mth_corp WHERE month_id=${year_month_id};")
fact_formatted=$(printf "%'.3f\n" $fact_total)

echo "get the total count for the incoming table"
kom_f_vod_order=$(hive -e "SELECT count(*) FROM incoming.kom_f_vod_order WHERE year_id=${year_id} and month_id=${month_id};")
kom_f_vod_order_formatted=$(printf "%'.3f\n" $kom_f_vod_order)

echo "get the total count for the incoming table"
kom_vod_order=$(hive -e "SELECT count(*) FROM incoming.kom_vod_order WHERE month_id=${year_month_id};")
kom_vod_order_formatted=$(printf "%'.3f\n" $kom_vod_order)


if [ -z "$fact_formatted" ]; then
        echo "total records for this monthid is empty or null"
        fact_formatted=0
fi

if [ -z "$kom_f_vod_order_formatted" ]; then
        echo "total records for this monthid is empty or null"
        kom_f_vod_order_formatted=0
fi

if [ -z "$kom_vod_order_formatted" ]; then
        echo "total records for this monthid is empty or null"
        kom_vod_order_formatted=0
fi

echo "fact_formatted={fact_formatted}"
echo "kom_f_vod_order_formatted={kom_f_vod_order_formatted}"
echo "kom_vod_order_formatted={kom_vod_order_formatted}"
