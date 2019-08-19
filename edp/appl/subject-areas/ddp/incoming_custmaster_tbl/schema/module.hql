--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_custmaster_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 05/02/2016
--#   Log File    : .../log/DDP_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/DDP_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          05/02/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
;

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name} 
 ( 
	ACCUMDEBIT                   DECIMAL(9,2)       COMMENT '',
	ADJCNT                       INT                COMMENT 'COUNT FOR THE ADJUSTMENT COMMENT',
	ANNIVDAY                     INT                COMMENT 'DAY OF THE MONTH SPECIFIED AS THE CUSTOMERIS S ANNIVERSARY DAY, 1-31',
	BADR                         VARCHAR(1)         COMMENT 'DESIGNATES BILLING ADDRESS FORMATS 1=BILLING ADDRESS ONLY-ADDIS L MAIL TO SVC ADDRS 2=MAILING ADDRESS - ALL MAIL TO THIS ADDRESS',
	BDATE                        DATE               COMMENT 'BILL DATE, THIS CUSTOMERIS S START OR STOP BILL DATE',
	BILLEDAMT					 DECIMAL(9,2)       COMMENT '',	
	BILLTHRUDATE                 DATE               COMMENT 'DATE UPDATED BY DDPF TO INDICATE THE BILL THROUGH PERIOD.',
	BPHONE                       VARCHAR(10)        COMMENT 'CUSTOMERIS S BUSINESS PHONE NUMBER ',
	C120                         DECIMAL(9,2)       COMMENT 'PORTION OF THE BALANCE THAT IS AGED 120 DAYS ',
	C30                          DECIMAL(9,2)       COMMENT ' PORTION OF THE BALANCE THAT IS AGED 30 DAYS',
	C60                          DECIMAL(9,2)       COMMENT 'PORTION OF THE BALANCE THAT IS AGED 60 DAYS',
	C90                          DECIMAL(9,2)       COMMENT 'PORTION OF THE BALANCE THAT IS AGED 90 DAYS',
	CAMPG                        VARCHAR(1)         COMMENT 'CAMPAIGN CODE FROM THE INSTALLATION, CODE36 TABLE 112',
	CHARGETHRUDATE               DATE               COMMENT 'THE LAST DATE THE CHARGING RUN WAS EXCUTED FOR THIS SUB.',
	CHRONICANNVDATE              DATE               COMMENT ' ' ,
	CINFO                        VARCHAR(1)         COMMENT 'CODE FOR CUSTOMER INFORMATION, CODE36 TABLE 116, USE CODE IS VIS = WILL BE VIP...EXEMPT FROM ',
	CLASS                        VARCHAR(1)         COMMENT 'CUSTOMER CLASS,  CODE36  TABLE 114',
	COITEM                       INT                COMMENT 'THE LEDGR ITEM NUMBER (LINE NUMBER. OF THE FIRST ITEM POSTED IN TWILIGHT',
	COLCNT                       INT                COMMENT 'COUNTER FOR CONTACT COMMENTS',
	COLST                        VARCHAR(1)         COMMENT 'COLLECTION STATUS, BLANK=NOT IN COLLECTIONS. IS 1IS =IN COLLECTIONS "2"=PENDING NPD/IN COLLECTIONS "3"=NOT IN COLLECTIONS BUT HAS A CUSTCOLLECT RECORD FOR PRODUCTS',
	COMTT                        VARCHAR(1)         COMMENT 'COMMENT FLAG Y=COMMENT RECORD ASSOCIATED WITH THIS CUSTOMER',
	CORP                         INT                COMMENT 'FIVE DIGIT SYSTEM NUMBER ASSIGNED TO EACH CORP.',
	CRDTPRORAT                   DECIMAL(9,2)       COMMENT 'IN THE MEMO CREDIT SCHEME IS THE SUM TOTAL OF THE CREDIT PRORATES TAKEN...(NOT DURING TWILIGHT.',
	CTYPE                        VARCHAR(1)         COMMENT 'CODE DESCRIBING CUSTOMER TYPE CODE36 TABLE 115, IF USE CODE IS VIS  = WILL BE VIP...EXEMPT FROM COLLECTIONS',
	CUR                          DECIMAL(9,2)       COMMENT 'MONEY IN THE CURRENT MONEY BUCKET ',
	CUST                         VARCHAR(2)         COMMENT 'CUSTOMER NUMBER. PART OF THE CUSTOMER ACCOUNT NUMBER.  REFER TO APPENDIX_DOC #2.',
	CUSTMAIL                     VARCHAR(1)         COMMENT 'CUSTOMER MAIL FLAG, AS DEFINED BY CODE 36 TABLE ',
	CUSTMASTER_SEQ               BIGINT                COMMENT ' ',
	CUSTOMER_TYPE                VARCHAR(1)         COMMENT ' ',
	CUSTPIN                      VARCHAR(4)         COMMENT 'CUSTOMER LEVEL PERSONAL IDENTIFICATION NUMBER NUMERIC',
	CVTR                         VARCHAR(1)         COMMENT 'CONVERTER FLAG, BLANK=NO BOXES, 1=AT LEAST ONE ADDRESSABLE BOX 2=NON-ADDRESSABLE BOX',
	CXRSN                        VARCHAR(1)         COMMENT 'REASON FOR CANCELLING THE INSTALL, CODE36 TABLE 133',
	CYCLE                        VARCHAR(1)         COMMENT 'IN CUSTMASTER = CUSTOMERIS S BILLING CYCLE, IN CYCLETABLE = VALID CYCLES FOR CORP',
	DCNT                         VARCHAR(1)         COMMENT 'CODE FOR DISCOUNTING THE CUSTOMERIS S SERVICES, CODE36 TABLE 121, ONE CODE CAN BE USED TO DENOTE TAX EXEMPTION (TAXEMPT IN CONTROLPARAMS TABLE.',
	DPST                         DECIMAL(9,2)       COMMENT 'AMOUNT OF MONEY HELD ON DEPOSIT FOR THIS CUSTOMER',
	DRIVERSLCNUM                 VARCHAR(21)        COMMENT 'DRIVERS LICENSE NUMBER',
	DRIVLICNSNUM                 VARCHAR(15)        COMMENT 'NO LONGER USED...SEE DRIVERSLCNUM BELOW',
	DRSN                         VARCHAR(1)         COMMENT ' ',
	EBILL                        VARCHAR(1)         COMMENT ' ',
	EBILL_EXPIRE_DATE            DATE               COMMENT ' ',
	EFTSFLG                      VARCHAR(1)         COMMENT 'FLAG INDICATING IF SELECTING ON EFTS CUSTOMER, 1=EFTS CUST, 2=PRE-NOTED, <BLANK>=NO',
	EQWRT                        DECIMAL(9,2)       COMMENT ' ',
	EXTSEQ                       BIGINT                COMMENT ' ',
	FASTRATES                    VARCHAR(360)       COMMENT ' RATE CODES FROM CUSTRATES STORED HORIZONTALLY CONCATENATED',
	FASTRATES420                 VARCHAR(420)       COMMENT ' ',
	FINBL                        VARCHAR(1)         COMMENT 'FINAL BILL SENT FLAG, 1=LAST BILL SENT 2=TRANSFER TYPE JOB',
	FNAME                        VARCHAR(8)         COMMENT 'CUSTOMERIS S FIRST NAME',
	FORM                         VARCHAR(1)         COMMENT 'TYPE OF STATEMENT TO SEND TO THIS CUSTOMER, CODE36 TABLE 124, S=STATEMENT, P=POSTCARD, Q=COUPON, N=NONE (EFTS CUST.',
	GGS_TRANS_TIMESTAMP          VARCHAR(26)        COMMENT 'GOLDEN GATE ASSIGNED DATE FOR WHICH THE TRANSACTION OCCURRED ON THE TANDEM ',
	HOLD                         INT                COMMENT 'HOLD OPTION, 0=NOT ON HOLD. 1=NO UPGRADES. 2=NO PPV ORDERING. 3=NO UPGRADES OR PPV. 4=NO TRANSFERS. 5=NO TRANSFERS OR UPGRADES. 6=NO TRANSFERS OR PPV 7=NO TRANSFERS, UPGRADES, OR PPV ORDERING',
	HOUSE                        VARCHAR(6)         COMMENT ' HOUSE NUMBER, ACCOUNT NUMBER, ALPHA/NUMERIC, REFER TO APENDIX_DOC #2',
	HSDS                         INT                COMMENT ' ',
	HWSEQ                        VARCHAR(1)         COMMENT 'CODE DESIGNATING HONEYWELL SEQUENCE NUMBER, LEGAL VALUES ARE 0-Z ',
	IDATE                        DATE               COMMENT 'DATE OF THE ORIGINAL INSTALL FOR THIS CUSTOMER',
	INCNTCTTYPE1                 VARCHAR(2)         COMMENT ' ',
	INCNTCTTYPE2                 VARCHAR(2)         COMMENT ' ',
	INCNTCTTYPE3                 VARCHAR(2)         COMMENT ' ',
	INIT                         VARCHAR(2)         COMMENT 'INITIALS',
	LANGUAGE                     VARCHAR(1)         COMMENT ' ',
	LASTIDATE                    DATE               COMMENT 'THE DATE THE CUST WAS LAST INSTALLED. THIS DATE WILL BE UPDATED WITH EACH INSTALL. (RESTART DATE. ',
	LASTPINCHGDTE_ID             DATE               COMMENT 'LAST PIN CHANGE DATE',
	LATEFEE                      DECIMAL(9,2)       COMMENT ' ',
	LNAME                        VARCHAR(13)        COMMENT 'CUSTOMERIS S LAST NAME',
	LOAD_DATE                    TIMESTAMP          COMMENT 'ORACLE ASSIGNED DATE OF WHEN THE RECORD WAS LOADED INTO THE ORACLE DATABASE  ',
	LPERUP                       VARCHAR(1)         COMMENT 'LAST PERIOD THIS RECORD WAS UPDATED, A-X ',
	LTWLZ                        VARCHAR(1)         COMMENT 'ITEMS POSTED IN TWILIGHT - BETWEEN CUTOFF AND UPDATE',
	MACHINE_ID                   VARCHAR(5)         COMMENT 'THE ID OF THE TANDEM THAT THE ORIGINAL RECORD WAS EXTRACTED FROM.',
	MEMOCRDT                     DECIMAL(9,2)       COMMENT 'IN THE MEMO CREDIT SCHEME IS THE TOTAL AMOUNT OF PAYMENTS,CREDIT ADJS,PPV CREDITS NOT YET POSTED.',
	NBRPAYS                      INT                COMMENT 'NUMBER OF PAY SERVICES FOR THIS CUSTOMER, 1-9',
	NEWCXRSN                     VARCHAR(3)         COMMENT ' ',
	NXTSLOT                      INT                COMMENT 'NXT LDGR SLOT(LINE NUMBER. TO INSERT NEW LDGR ITEMOR NXT OPEN SLOT FOR ASSIGNING JOBS TO CDW',
	OP_TYPE                      VARCHAR(1)         COMMENT 'GOLDEN GATE ASSIGNED OPERATION TYPE THAT DESCRIPBES THE ORIGINAL OPERATION THE',
	OTLCNT                       INT                COMMENT 'NUMBER OF ACTIVE OUTLETS FOR THIS CUSTOMER',
	OUTCNTCTTYPE1                VARCHAR(2)         COMMENT ' ',
	OUTCNTCTTYPE2                VARCHAR(2)         COMMENT ' ',
	OUTCNTCTTYPE3                VARCHAR(2)         COMMENT ' ',
	P80                          VARCHAR(1)         COMMENT 'FLAG INDICATING CUSTOMER IS ON DDPF MASTER, 1=ON DDPF',
	`PARTITION`                  VARCHAR(2)         COMMENT 'DATA HASHING VALUE FOR DATABASE MANAGEMENT - EQUALS THE LAST 2 DIGITS OF THE HOUSE #',
	PINCHGOPR                    VARCHAR(3)         COMMENT 'OPERATOR WHO LAST CHANGED CUSTOMER PIN',
	PPV_TOTAL                    DECIMAL(9,2)       COMMENT ' ',
	PPVDT                        DATE               COMMENT 'LAST PPV PURCHASE DATE',
	PPVLED                       INT                COMMENT 'LEDGER ITEM NUMBER THAT HOLDS PPV CHARGES FOR THE CURRENT PERIOD',
	PRODUCT_DPST                 DECIMAL(9,2)       COMMENT ' ',
	PVCLB                        VARCHAR(1)         COMMENT 'PPV CLUB DISCOUNT MEMBERSHIP, VALUES 1-7',
	RAREACD                      INT                COMMENT 'RESIDENCE/HOME PHONE AREA CODE.',
	RATE                         DECIMAL(8,2)       COMMENT 'MONTHLY RATE',
	REM                          VARCHAR(1)         COMMENT 'REMINDER SCHEME FOR THIS CUSTOMER, A-D ',
	RPHON                        VARCHAR(8)         COMMENT 'CUSTOMERIS S RESIDENCE PHONE NUMBER',
	RPHONAREA                    VARCHAR(1)         COMMENT '36 CODE TABLE TIED TO THE RESIDENCE/HOME PHONE NUMBER. (USED TO BE 8TH DIGIT OF PHONE.',
	SLSREP                       VARCHAR(3)         COMMENT 'SALES REP WHO SOLD THIS ACCOUNT',
	SRSN                         VARCHAR(1)         COMMENT 'SALES REASON FROM THE INSTALLATION, CODE36 TABLE 113',
	SSN   						 VARCHAR(9)        COMMENT '',
	STAT                         VARCHAR(1)         COMMENT 'STATUS FOR CUST - 1=P/IN 2=P/RS 3=P/CH 4=P/DS 5=ACTIVE 6=DISC 7=X/IN, REFUND STATUS 1,2,3,D,H,P,S,T,W EVENT DESCRIPTION TABLE 1-4',
	STMTCYCLE                    VARCHAR(1)         COMMENT ' CYCLE FOR THIS STATEMENT IMAGE, A-D',
	STMTPLAN                     INT                COMMENT 'PLAN # FOR LAST STATEMENT SENT THIS CUSTOMER ',
	T_MODE                       VARCHAR(1)         COMMENT 'FREQUENCY OF STATEMENT SENT TO THE CUSTOMER, CODE36 TABLE 125, 0=NONE, 1=MONTHLY, 2=BI-MONTHLY, 3=QUARTERLY, 6=SEMI-ANNUALLY, A=ANNUALLY',
	TALLYBUCKET                  INT                COMMENT 'TELLS WHICH BUCKET IN THE CUSTOMER TALLY TABLE IS CURRENT. 0 - 24 ',
	TDD                          VARCHAR(1)         COMMENT ' ',
	TITLE                        VARCHAR(2)         COMMENT 'TITLE ASSOCIATED WITH CUSTOMERIS S NAME OR NAME GIVEN A MAP TABLE OR GROUP TITLE FOR RATE INCREASE OR, AWARD TITLE, ALPHA-NUMERIC',
	TTY                          VARCHAR(1)         COMMENT ' ',
	TVTYP                        VARCHAR(1)         COMMENT 'TV TYPE, CODE36 TABLE 118, USE CODE IS VIS  = WILL BE VIP...EXEMPT FROM',
	TWLPRORATE                   DECIMAL(9,2)       COMMENT 'IN MEMO CREDIT SCHEME IS THE SUM TOTAL OF THE CREDIT PRORATES DURING IS TWILIGHTIS',
	VIP                          VARCHAR(1)         COMMENT 'VIP FLAG IN THE CUSTOMER TO EXCLUDE THE ACCOUNT FROM COLLECTIONS. Y=YES',
	VOIP_STATUS                  INT                COMMENT ' ',
	WIRELESS                     INT                COMMENT ' ',
	WRT                          DECIMAL(9,2)       COMMENT 'AMOUNT OF MONEY THAT HAS BEEN WRITTEN-OFF FOR THISACCOUNT AND NOT RECOVERED YET IS THE AMT OF THE WRITEOFF ADJ(DEBIT BALANCE AMT WILL HAVE CREDIT AMT IN THE FIELD. OR 1=WRITE TO HS '
)
PARTITIONED BY (LOAD_YEAR STRING,
                LOAD_MONTH STRING,
                LOAD_DAY STRING)
STORED AS AVRO
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################