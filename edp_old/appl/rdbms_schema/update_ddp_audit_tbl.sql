ALTER TABLE ddp_audit
ADD COLUMN table_type TEXT;

update ddp_audit
set table_type = 'medium'
where table_name in ('edp_auxcust','edp_auxhouse','edp_code36','edp_code95','edp_code999','edp_custbilladdr','edp_ratecodes','edp_ratepricearea','edp_ratepricelevel','edp_raterptctrs','edp_rptctrs','edp_slsmn','edp_techs','edp_zipmaster');

update ddp_audit
set table_type = 'low'
where table_name in ('edp_controlparams');

update ddp_audit
set table_type = 'large'
where table_name in ('edp_boxinvtry','edp_custmaster','edp_custrates','edp_housemaster','edp_wipcustrate','edp_wipmaster','edp_wipoutlet');