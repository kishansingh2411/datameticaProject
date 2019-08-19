CREATE OR REPLACE VIEW 
   smith_channel_tuning.d_channel_name
AS 
SELECT 
   b.channel_name_id, 
   b.channel_name, 
   b.sourced_from_system, 
   b.last_updated_by_system, 
   b.dtm_created, 
   b.dtm_last_updated, 
   b.ins_stb_file_control_id,    
   b.upd_stb_file_control_id, 
   b.tms_network_id, 
   b.tms_network_nbr, 
   b.tms_network_time_zone, 
   b.tms_network_name, 
   b.tms_fcc_call_sign, 
   b.tms_network_affiliate,  
   b.tms_network_city, 
   b.tms_network_state, 
   b.tms_network_zip_code, 
   b.tms_network_country, 
   b.tms_designates_market_name, 
   b.tms_designates_market_nbr,  
   b.tms_fcc_channel_nbr, 
   b.tms_network_language, 
   b.channel_descr, 
   b.cvc_call_sign, 
   b.peg_ind, 
   b.channel_category, 
   b.woodbury_lineup_channel_nbr,  
   b.parent_network, 
   b.network_group, 
   b.network_alias, 
   b.program_exclusion_ind, 
   b.insertable_ind, 
   b.broadcast_flag, 
   b.strata_name, 
   b.tgt_dtm_created,  
   b.tgt_dtm_last_updated, 
   b.strata_station_id   
FROM smith_channel_tuning.smith_t_d_channel_name b
;