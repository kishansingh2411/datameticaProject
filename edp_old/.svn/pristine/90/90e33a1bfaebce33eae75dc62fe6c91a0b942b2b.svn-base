-- Creating Key_params entries for Dimension and Fact Appworks chain.

INSERT INTO t_key_params 
(key_param_id, job_key, param_desc
, param_numeric, param_varchar
, param_dtm_start, param_dtm_end
, commit_point, output_dir
, dtm_created, dtm_last_updated
, incremental_in_hrs, process_lag_in_mins
) VALUES (500601,500601,'INCRE LOAD FACT AM TABLES'
,NULL,NULL,current_date,current_date,NULL
,NULL,current_date,NULL,720,NULL);

INSERT INTO t_key_params 
(key_param_id, job_key, param_desc
, param_numeric, param_varchar, param_dtm_start
, param_dtm_end, commit_point
, output_dir, dtm_created, dtm_last_updated
, incremental_in_hrs, process_lag_in_mins
) VALUES (500600,500600,'TRUNC & LOAD DIMENISON AM TABLES'
,NULL,NULL,current_date,current_date
,NULL,NULL,current_date,NULL,24,NULL);

--Creating entries for Dimension and Fact tables.

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_table,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100183,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','T_D_PERIOD',
'redshift_cvrsam_oaimgr.cfg','s3://cvcbisamdev','edp_offload/t_d_period','NA','NA',
'redshift_cvrsam_oaimgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);


insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_table,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100184,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','T_D_CHANNEL_NAME',
'redshift_cvrsam_oaimgr.cfg','s3://cvcbisamdev','edp_offload/t_d_channel_name','NA','NA',
'redshift_cvrsam_oaimgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);


insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_table,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100185,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','T_D_STB_COLLECTION_TASK',
'redshift_cvrsam_oaimgr.cfg','s3://cvcbisamdev','edp_offload/t_d_stb_collection_task','NA','NA',
'redshift_cvrsam_oaimgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_table,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100186,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','T_D_HOUSEHOLD_DEVICE',
'redshift_cvrsam_oaimgr.cfg','s3://cvcbisamdev','edp_offload/t_d_household_device','NA','NA',
'redshift_cvrsam_oaimgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_table,source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100187,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','T_D_HOUSEHOLD_ATTRIBUTE','redshift_cvrsam_oaimgr.cfg',
's3://cvcbisamdev','edp_offload/t_d_household_attribute','NA','NA','redshift_cvrsam_oaimgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_table,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100188,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','T_D_RES_DEMOGRAPHIC',
'redshift_cvrsam_oaimgr.cfg','s3://cvcbisamdev','edp_offload/t_d_res_demographic','NA','NA',
'redshift_cvrsam_oaimgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_table,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100189,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','T_D_TMS_PROGRAM',
'redshift_cvrsam_oaimgr.cfg','s3://cvcbisamdev','edp_offload/t_d_tms_program','NA','NA',
'redshift_cvrsam_oaimgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_query,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100190,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','SELECT HOUSEHOLD_DEVICE_ID, QUARTER_HOUR_OF_DAY_OFFSET, STB_COLLECTION_TASK_ID, CHANNEL_NAME_ID
, TMS_NETWORK_ID, TMS_PROGRAM_ID, SEGMENT_START_TIME, SEGMENT_END_TIME, DTM_PROGRAM_START
, DTM_PROGRAM_END, SEGMENTED_DURATION, CHANNEL_TUNE_DURATION, STB_FILE_CONTROL_ID
, LOAD_DATE, DTM_CREATED, DTM_CHANNEL_TUNING_CREATED, DTM_LAST_UPDATED
, HOUSEHOLD_ID, CHANNEL_NBR, SAMPLE_ID, cast(SEGMENT_DATE as date) as SEGMENT_DATE, DTM_TUNED_TO_STARTED
, DTM_TUNED_TO_ENDED, TUNING_SOURCE_ID, TUNING_TYPE_ID, PLAYBACK_DAY, QTR_SUM_DEV_DURATION
, QTR_SUM_DEV_CHN_DUR, QTR_SUM_DEV_CHN_PROG_DUR, BROADCAST_WEEK_ID, BROADCAST_MONTH_ID
, CALENDAR_WEEK_ID, CALENDAR_MONTH_ID, DAY_OF_WEEK_ID, CHN_PROG_CONSEC_DUR
 FROM AMHMGR.NZ_T_F_SPLIT_CHANNEL_TUNING_RST 
 WHERE SEGMENT_DATE IN (SELECT PERIOD_DATE FROM CVBMGR.D_PERIOD WHERE BROADCAST_MONTH_ID = $1$)',
'redshift_cvrsam_cvbmgr.cfg','s3://cvcbisamdev','edp_offload/nz_t_f_split_channel_tuning_rst','NA','NA',
'redshift_cvrsam_cvbmgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_query,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100191,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','SELECT SPLIT_DVR_USAGE_ID, HOUSEHOLD_ID, HOUSEHOLD_DEVICE_ID, DTM_RECORDING_START, 
cast(SEGMENT_DATE as date) as SEGMENT_DATE, DTM_TUNED_TO_STARTED, DTM_TUNED_TO_ENDED, DTM_SEGMENT_START, DTM_SEGMENT_END
, CHANNEL_TUNE_DURATION_SEC_AMT, SEGMENTED_DURATION_SEC_AMT, QUARTER_HOUR_OF_DAY_OFFSET
, SAMPLE_ID, STB_COLLECTION_TASK_ID, CHANNEL_NAME_ID, CHANNEL_NBR, TMS_NETWORK_ID
, TMS_PROGRAM_ID, DTM_PROGRAM_START, DTM_PROGRAM_END, SRC_TITLE, REMOTE_CONTROL_KEY_ID
, DTM_ACTIVITY_START, DTM_ACTIVITY_END, ACTIVITY_DURATION_SEC_AMT, STB_FILE_CONTROL_ID
, PLAYBACK_DAY, OP_REPEAT_FLAG, LOAD_DATE, DTM_CREATED, DTM_SRC_REC_CREATED
, DTM_LAST_UPDATED, TUNING_SOURCE_ID, TUNING_TYPE_ID, QTR_SUM_DEV_DURATION
, QTR_SUM_DEV_CHN_DUR, QTR_SUM_DEV_CHN_PROG_DUR, BROADCAST_WEEK_ID, BROADCAST_MONTH_ID
, CALENDAR_WEEK_ID, CALENDAR_MONTH_ID, DAY_OF_WEEK_ID, CHN_PROG_CONSEC_DUR
FROM CVBMGR.T_CVB_F_SPLIT_DVR_USAGE 
WHERE SEGMENT_DATE IN (SELECT PERIOD_DATE FROM CVBMGR.D_PERIOD WHERE BROADCAST_MONTH_ID = $1$)',
'redshift_cvrsam_cvbmgr.cfg','s3://cvcbisamdev','edp_offload/t_cvb_f_split_dvr_usage','NA','NA',
'redshift_cvrsam_cvbmgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_query,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100192,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','SELECT HOUSEHOLD_ID, HOUSEHOLD_IPDEVICE_ID, 
cast(SEGMENT_DATE as date) as SEGMENT_DATE, SAMPLE_ID, QUARTER_HOUR_OF_DAY_OFFSET, CHANNEL_NAME_ID
, TMS_NETWORK_ID, TMS_PROGRAM_ID, SEGMENT_START_TIME, SEGMENT_END_TIME, DTM_PROGRAM_START, DTM_PROGRAM_END
, SEGMENTED_DURATION, CHANNEL_TUNE_DURATION, DTM_TUNED_TO_STARTED, DTM_TUNED_TO_ENDED, SRC_CHANNEL_NAME
, AS_USER_ID, APP_VERSION, AS_CHANNEL_TUNING_ID, DTM_CHANNEL_TUNING_CREATED, LOAD_DATE, DTM_CREATED
, DTM_LAST_UPDATED, TUNING_SOURCE_ID, TUNING_TYPE_ID, PLAYBACK_DAY, QTR_SUM_DEV_CHN_DUR, QTR_SUM_DEV_CHN_PROG_DUR
, QTR_SUM_DEV_DURATION, BROADCAST_WEEK_ID, BROADCAST_MONTH_ID, CALENDAR_WEEK_ID, CALENDAR_MONTH_ID, DAY_OF_WEEK_ID
, CHN_PROG_CONSEC_DUR FROM CVBMGR.T_F_SPLIT_IPCHANNEL_TUNING 
WHERE SEGMENT_DATE IN (SELECT PERIOD_DATE FROM CVBMGR.D_PERIOD WHERE BROADCAST_MONTH_ID = $1$)',
'redshift_cvrsam_cvbmgr.cfg','s3://cvcbisamdev','edp_offload/t_f_split_ipchannel_tuning','NA','NA',
'redshift_cvrsam_cvbmgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_query,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100193,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','SELECT SPLIT_MRDVR_USAGE_ID, HOUSEHOLD_ID, 
HOUSEHOLD_DEVICE_ID, DTM_RECORDING_START, cast(SEGMENT_DATE as date) as SEGMENT_DATE, DTM_TUNED_TO_STARTED, 
DTM_TUNED_TO_ENDED, DTM_SEGMENT_START, DTM_SEGMENT_END, CHANNEL_TUNE_DURATION_SEC_AMT, SEGMENTED_DURATION_SEC_AMT,
QUARTER_HOUR_OF_DAY_OFFSET, SAMPLE_ID, STB_COLLECTION_TASK_ID, CHANNEL_NAME_ID, CHANNEL_NBR, TMS_NETWORK_ID, 
TMS_PROGRAM_ID, DTM_PROGRAM_START, DTM_PROGRAM_END, STB_ASSET_ID, REMOTE_CONTROL_KEY_ID, DTM_ACTIVITY_START,
DTM_ACTIVITY_END, ACTIVITY_DURATION_SEC_AMT, PLAYBACK_DAY, OP_REPEAT_FLAG, LOAD_DATE, DTM_CREATED, 
DTM_SRC_REC_CREATED, DTM_LAST_UPDATED, LAST_UPDATED_BY, TUNING_SOURCE_ID, TUNING_TYPE_ID, QTR_SUM_DEV_DURATION,
QTR_SUM_DEV_CHN_DUR, QTR_SUM_DEV_CHN_PROG_DUR, BROADCAST_WEEK_ID, BROADCAST_MONTH_ID, CALENDAR_WEEK_ID,
CALENDAR_MONTH_ID, DAY_OF_WEEK_ID, CHN_PROG_CONSEC_DUR FROM CVBMGR.T_CVB_F_SPLIT_MRDVR_USAGE 
WHERE SEGMENT_DATE IN (SELECT PERIOD_DATE FROM CVBMGR.D_PERIOD WHERE BROADCAST_MONTH_ID = $1$)',
'redshift_cvrsam_cvbmgr.cfg','s3://cvcbisamdev','edp_offload/t_cvb_f_split_mrdvr_usage','NA','NA',
'redshift_cvrsam_cvbmgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_query,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100194,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','SELECT HOUSEHOLD_DEVICE_ID, 
QUARTER_HOUR_OF_DAY_OFFSET, STB_COLLECTION_TASK_ID, CHANNEL_NAME_ID
, TMS_NETWORK_ID, TMS_PROGRAM_ID, SEGMENT_START_TIME, SEGMENT_END_TIME, DTM_PROGRAM_START
, DTM_PROGRAM_END, SEGMENTED_DURATION, CHANNEL_TUNE_DURATION, STB_FILE_CONTROL_ID
, LOAD_DATE, DTM_CREATED, DTM_CHANNEL_TUNING_CREATED, DTM_LAST_UPDATED
, HOUSEHOLD_ID, CHANNEL_NBR, SAMPLE_ID, cast(SEGMENT_DATE as date) as SEGMENT_DATE, DTM_TUNED_TO_STARTED
, DTM_TUNED_TO_ENDED, TUNING_SOURCE_ID, TUNING_TYPE_ID, PLAYBACK_DAY, QTR_SUM_DEV_DURATION
, QTR_SUM_DEV_CHN_DUR, QTR_SUM_DEV_CHN_PROG_DUR, BROADCAST_WEEK_ID, BROADCAST_MONTH_ID
, CALENDAR_WEEK_ID, CALENDAR_MONTH_ID, DAY_OF_WEEK_ID, CHN_PROG_CONSEC_DUR
 FROM CVBMGR.NZ_T_F_SPLIT_CHANNEL_TUNING_6MTH
 WHERE SEGMENT_DATE IN (SELECT PERIOD_DATE FROM CVBMGR.D_PERIOD WHERE BROADCAST_MONTH_ID = $1$)',
'redshift_cvrsam_cvbmgr.cfg','s3://cvcbisamdev','edp_offload/nz_t_f_split_channel_tuning_6mth','NA','NA',
'redshift_cvrsam_cvbmgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);

insert into aws_job_master
(aws_job_Id,job_desc,transfer_type,source_system,source_query,
source_config,target_bucket,target_bucket_dir,target_system,target_table,target_config,export_option,
import_option,parallel_processes,truncate_target,delimiter_char,max_errors,job_status,last_run_date)
values
(100195,'DTT load from redshift to s3 - T_HOUSEHOLD_LIST_TEMP_DR2PR','DTT','REDSHIFT','SELECT HOUSEHOLD_DEVICE_ID, 
QUARTER_HOUR_OF_DAY_OFFSET, STB_COLLECTION_TASK_ID, CHANNEL_NAME_ID
, TMS_NETWORK_ID, TMS_PROGRAM_ID, SEGMENT_START_TIME, SEGMENT_END_TIME, DTM_PROGRAM_START
, DTM_PROGRAM_END, SEGMENTED_DURATION, CHANNEL_TUNE_DURATION, STB_FILE_CONTROL_ID
, LOAD_DATE, DTM_CREATED, DTM_CHANNEL_TUNING_CREATED, DTM_LAST_UPDATED
, HOUSEHOLD_ID, CHANNEL_NBR, SAMPLE_ID, cast(SEGMENT_DATE as date) as SEGMENT_DATE, DTM_TUNED_TO_STARTED
, DTM_TUNED_TO_ENDED, TUNING_SOURCE_ID, TUNING_TYPE_ID, PLAYBACK_DAY, QTR_SUM_DEV_DURATION
, QTR_SUM_DEV_CHN_DUR, QTR_SUM_DEV_CHN_PROG_DUR, BROADCAST_WEEK_ID, BROADCAST_MONTH_ID
, CALENDAR_WEEK_ID, CALENDAR_MONTH_ID, DAY_OF_WEEK_ID, CHN_PROG_CONSEC_DUR
 FROM CVBMGR.NZ_T_F_SPLIT_CHANNEL_TUNING_CURR
 WHERE SEGMENT_DATE IN (SELECT PERIOD_DATE FROM CVBMGR.D_PERIOD WHERE BROADCAST_MONTH_ID = $1$)',
'redshift_cvrsam_cvbmgr.cfg','s3://cvcbisamdev','edp_offload/nz_t_f_split_channel_tuning_curr','NA','NA',
'redshift_cvrsam_cvbmgr.cfg','UNLDS3','ONLYS3',NULL,'N','^',0,'IDLE',current_Date);