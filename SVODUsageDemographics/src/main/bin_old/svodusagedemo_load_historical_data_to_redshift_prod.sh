#!/bin/bash
########################################################################################################################
#
#   Program name: svodusagedemo_load_history_data_to_redshift_prod.sh
#   Program type: Unix Shell script
#   Author      : Kriti Singh
#   Date        : 05/29/2016
#
#   Description :
#
#   Usage       :
#
#vod
########################################################################################################################

#Copy data from redshift dev database to redshift prod database
s3cmd del --recursive s3://cvcbis/svodusagedemo/f_vod_orders_mth_corp

psql "host=$DEVDBHOST port=$DEVDBPORT dbname=$DEVDBNAME user=$DEVDBUSER password=$DEVDBPASS" -F --no-allign -w -c "
unload ('select * from f_vod_orders_mth_corp') to 's3://cvcbis/svodusagedemo/f_vod_orders_mth_corp/' credentials 'aws_access_key_id=$AWS_ACCESS_KEY_ID;aws_secret_access_key=$AWS_SECRET_ACCESS_KEY' NULL as 'null' delimiter '\t' allowoverwrite"

##copy data from s3 to redshift dev
psql "host=$DBHOST port=$DBPORT dbname=$DBNAME user=$DBUSER password=$DBPASS" -F --no-allign -c "copy $DBUSER.f_vod_orders_mth_corp(month_id,studio_id,title_id,subscription_name_id,subscription_type_id,hd_flag,genre_id,corp_id,preview_ind,trailer_ind,ecohort_code_id,ethnic_code_id,vod_play_time_seconds,
 vod_fast_forward_cnt,vod_rewind_cnt,vod_pause_cnt,vod_order_amt,vod_unique_customers_cnt,vod_unique_boxes_cnt,vod_orders_cnt,kom_last_modified_date) from 's3://cvcbis/svodusagedemo/f_vod_orders_mth_corp' credentials 'aws_access_key_id=$AWS_ACCESS_KEY_ID;aws_secret_access_key=$AWS_SECRET_ACCESS_KEY' timeformat 'auto';"