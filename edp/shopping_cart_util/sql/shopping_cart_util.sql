CREATE TABLE F_CART_PRODUCT_LIST 
(
  CART_ID VARCHAR2(20) NOT NULL 
, SEQUENCE_ID VARCHAR2(20) NOT NULL 
, CHARGE_TYPE_X9 VARCHAR2(20) 
, DURATION_X9 INTEGER 
, PART_TYPE_X9 VARCHAR2(20) 
, PREMIUM_SERVICE_FLAG_X9 VARCHAR2(20) 
, PRICE_X9 NUMBER(10,4) 
, PRODUCT_TYPE_X9 VARCHAR2(20) 
, PROMO_PRICE_X9 NUMBER(10,4) 
, PROMOTION_IND_X9 VARCHAR2(20) 
, RATE_CODE_X9 VARCHAR2(20) 
, STANDARD_RATE_X9 VARCHAR2(20) 
, WIN_DURATION_X9 INTEGER
, DTM_LAST_UPDATED_SRC TIMESTAMP(6)
, DTM_CREATED DATE 
);


CREATE TABLE F_OFFER_ID
(
  CART_ID VARCHAR2(20) NOT NULL 
, SEQUENCE_ID VARCHAR2(20)
, OFFER_ID VARCHAR(20) NOT NULL
, OFFER_NAME VARCHAR2(250) 
, OFFER_DESCRIPTION VARCHAR2(500) 
, DTM_LAST_UPDATED_SRC TIMESTAMP(6)
, DTM_CREATED DATE 
, NBR_ROWS INTEGER
);

CREATE TABLE WEB_SERVICE_LOG
(
 SEQUENCE_ID VARCHAR2(20)
, KEY_PARAM_ID NUMBER
, H_SHOPPING_CART_ID_BEGIN NUMBER 
, H_SHOPPING_CART_ID_END NUMBER
, CART_ID_CALLED_COUNT NUMBER(6) 
, CART_ID_SUCC_COUNT NUMBER(6)
, CART_ID_NORESP_COUNT NUMBER(6)
, DTM_START DATE
, DTM_END DATE
, DTM_CREATED DATE
);

CREATE TABLE F_CART_MISSING_INFO
(
  H_SHOPPING_CART_ID NUMBER
, SEQUENCE_ID VARCHAR2(20) 
, CART_ID VARCHAR2(20) NOT NULL
, PRODUCT_IS_NULL NUMBER(1)
, OFFER_IS_NULL NUMBER(1)
);

INSERT INTO KEY_PARAMS VALUES (16075, 16075, 'uow_java_dd16075_retrieve_shopping_cart', 7200, null, null, null, 0, '/appfdr/fdr/', sysdate, null);