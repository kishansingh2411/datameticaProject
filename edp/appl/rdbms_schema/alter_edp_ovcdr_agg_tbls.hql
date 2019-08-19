
ALTER TABLE WORK_OVCDR.WORK_A_DOM_NSN_IN_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE GOLD_OVCDR.GOLD_A_DOM_NSN_IN_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE WORK_OVCDR.WORK_A_DOM_NSN_OUT_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE GOLD_OVCDR.GOLD_A_DOM_NSN_OUT_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE WORK_OVCDR.WORK_A_INT_NSN_CALL_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE GOLD_OVCDR.GOLD_A_INT_NSN_CALL_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE WORK_OVCDR.WORK_A_INT_RAD_CALL_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE GOLD_OVCDR.GOLD_A_INT_RAD_CALL_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE WORK_OVCDR.WORK_A_VMA_NSN_CALL_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;

ALTER TABLE GOLD_OVCDR.GOLD_A_VMA_NSN_CALL_USAGE
   ADD COLUMNS (BHV_INDICATOR VARCHAR(1)   COMMENT '', 
       CUST_ACCT_NUMBER      VARCHAR(15)  COMMENT '',
       SOURCE_SYSTEM_ID      TINYINT      COMMENT ''
   )
;