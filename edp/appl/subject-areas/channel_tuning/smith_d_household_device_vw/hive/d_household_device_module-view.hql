CREATE OR REPLACE VIEW 
   smith_channel_tuning.d_household_device
AS 
SELECT 
   b.household_device_id, 
   b.household_key1, 
   b.device_key1, 
   b.device_key2, 
   b.device_make, 
   b.device_model, 
   b.zip_code, 
   b.sourced_from_system, 
   b.last_updated_by_system, 
   b.dtm_created, 
   b.dtm_last_updated, 
   b.ins_stb_file_control_id, 
   b.upd_stb_file_control_id, 
   b.created_by, 
   b.household_type, 
   b.household_type_descr, 
   b.household_state, 
   b.household_state_descr, 
   b.last_updated_by, 
   b.dtm_of_disconnect, 
   b.disconnect_code, 
   b.disconnect_descr, 
   b.device_state, 
   b.dtm_provisioned, 
   b.household_key2, 
   b.household_key3, 
   b.household_key4, 
   b.household_key5, 
   b.corp, 
   b.corp_franchise_tax_area, 
   b.non_disclosure_ind, 
   b.dtm_of_non_disclosure_request, 
   b.household_id, 
   b.corp_name, 
   b.corp_franchise_tax_area_name, 
   b.household_key6, 
   b.zip_code6, 
   b.census, 
   b.mdu, 
   b.dtm_original_install, 
   b.op_type, 
   b.tgt_dtm_created, 
   b.tgt_dtm_last_updated, 
   b.device_ext_key1, 
   b.device_ext_key2, 
   b.household_ext_key1, 
   b.household_ext_key2
FROM 
 smith_channel_tuning.smith_t_d_household_device b
;