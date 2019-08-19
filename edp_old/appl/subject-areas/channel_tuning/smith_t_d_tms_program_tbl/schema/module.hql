--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(gold_nz_t_f_split_channel_tuning_rst_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : ...log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/30/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
   TMS_PROGRAM_ID                  INT          COMMENT 'Unique identification of a program (what ever that means).  System assigned non-intrinsic number based on the TMS_DATABASE_KEY.',
   TMS_DATABASE_KEY                VARCHAR(14)        COMMENT 'Unique program identifier, necessary to reference movies, shows, episodes, sports.  The format of the key is: The forst two characters in the database key represent where the TMS database record is stored.  MV = movie, SH = show EP = episode, SP = sport. The next 12 characters will be the unique number within that database.  Sourced from: PROGREC.TXT.TF_DATABASE_KEY',
   PROGRAM_CLASSIFICATION          VARCHAR(20)        COMMENT 'This is derived data substr(TF_DATABASE_KEY,1,2).  Sourced from first two characters of the TMS_DATABASE_KEY.  Domain: ''MV'' = Movie ''SH'' = Showing ''EP'' = Episode ''SP'' = Sport',
   TITLE                           VARCHAR(400)       COMMENT 'Program title of a movie, show, episode or sports event.  Sourced from: PROGREC.TXT.TF_TITLE.  2008.08.20: A value of NULL or spaces should be transformed to "Unknown"',
   REDUCED_TITLE_1                 VARCHAR(255)       COMMENT 'Shortened version of the program title.  Sourced from: PROGREC.TXT.TF_REDUCED_TITLE_1.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   REDUCED_TITLE_2                 VARCHAR(255)       COMMENT 'Shortened version of the program title.  Sourced from: PROGREC.TXT.TF_REDUCED_TITLE_2.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   REDUCED_TITLE_3                 VARCHAR(255)       COMMENT 'Shortened version of the program title.  Sourced from: PROGREC.TXT.TF_REDUCED_TITLE_3.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   REDUCED_TITLE_4                 VARCHAR(255)       COMMENT 'Shortened version of the program title.  Sourced from: PROGREC.TXT.TF_REDUCED_TITLE_4.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   ALT_TITLE                       VARCHAR(255)       COMMENT 'Alias name for program title: the title "Paid Programming" is stored here.  Sourced from: PROGREC.TXT.TF_ALT_TITLE.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   REDUCED_DESCR_1                 VARCHAR(255)       COMMENT 'Shorter version of a programs original description.  Sourced from: PROGREC.TXT.TF_REDUCED_DESC_1.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   REDUCED_DESCR_2                 VARCHAR(255)       COMMENT 'Shorter version of a programs original description.  Sourced from: PROGREC.TXT.TF_REDUCED_DESC_2.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   REDUCED_DESCR_3                 VARCHAR(255)       COMMENT 'Shorter version of a programs original description.  Sourced from: PROGREC.TXT.TF_REDUCED_DESC_3.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   ADULT_SITUATION                 VARCHAR(255)       COMMENT 'Notation of adult content in movies, shows and episodes.  Sourced from: PROGREC.TXT.TF_ADVISORY_DESC_1.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   GRAPHIC_LANGUAGE                VARCHAR(255)       COMMENT 'Notation of explicit language in movies, shows and episodes.  Sourced from: PROGREC.TXT.TF_ADVISORY_DESC_2.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   NUDITY                          VARCHAR(100)       COMMENT 'Notation of nudity in movies, shows and episodes.  Sourced from: PROGREC.TXT.NUDITY.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   VIOLENCE                        VARCHAR(100)       COMMENT 'Notation of violence in movies, shows and episodes.  Sourced from: PROGREC.TXT.TF_ADVISORY_DESC_4.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   SEXUAL_CONTENT                  VARCHAR(100)       COMMENT 'Notation of sexual content in movies, shows and episodes.  Sourced from: PROGREC.TXT.TF_ADVISORY_DESC_5.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   CONSENTUAL_BEHAVIOR             VARCHAR(100)       COMMENT 'Notation of ravies, shows and episodes.  Sourced from: PROGREC.TXT.TF_ADVISORY_DESC_6.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   DESCR_1                         VARCHAR(255)       COMMENT '',
   YEAR                            VARCHAR(7)        COMMENT '',
   MPAA_RATING                     VARCHAR(5)        COMMENT '',
   STAR_RATING                     VARCHAR(5)        COMMENT '',
   RUN_TIME                        VARCHAR(4)        COMMENT '',
   COLOR_CODE                      VARCHAR(15)        COMMENT '',
   PROGRAM_LANGUAGE                VARCHAR(40)        COMMENT 'Language description) of a program.  Sourced from: PROGREC.TXT.TF_PROGRAM_LANGUAGE.  2008.08.08: A value of NULL or spaces should be transformed to ''N/A''',
   COUNTRY_OF_ORIGIN               VARCHAR(15)        COMMENT 'Used in movie languish between domestic and foreign films.  Also known as country of origin.  Sourced from: PROGREC.TXT.TF_ORG_COUNTRY.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   MADE_FOR_TV                     VARCHAR(3)        COMMENT '',
   SOURCE_TYPE                     VARCHAR(10)        COMMENT '',
   SHOW_TYPE                       VARCHAR(30)        COMMENT '',
   HOLIDAY                         VARCHAR(30)        COMMENT '',
   SYNDICATED_EPISODE_NBR          VARCHAR(20)        COMMENT '',
   ALT_SYNDICATED_EPISODE_NBR      VARCHAR(20)        COMMENT '',
   EPISODE_TITLE                   VARCHAR(150)       COMMENT '',
   USER_DATA_1                     VARCHAR(255)       COMMENT '',
   USER_DATA_2                     VARCHAR(255)       COMMENT '',
   DESCR_2                         VARCHAR(255)       COMMENT '',
   REDUCED_DESCR                   VARCHAR(200)       COMMENT '',
   ORG_STUDIO                      VARCHAR(25)        COMMENT '',
   GAME_DATE                       VARCHAR(8)        COMMENT '',
   GAME_TIME                       VARCHAR(4)        COMMENT '',
   GAME_TIME_ZONE                  VARCHAR(45)        COMMENT 'Time zone of GAME_TIME not necessarlity the time zone of the event.  Sourced from: PROGREC.TXT.TF_GAME_TIME_ZONE.  2008.08.08: A value of NULL or spaces should be transformed to "N/A"',
   ORG_AIR_DATE                    VARCHAR(8)        COMMENT 'Original air date for the program.  Sourced from: PROGREC.TXT.TF_ORG_AIR_DATE.  2008.08.20: A value of NULL ores should be transformed to "N/A"',
   USER_DATA_3                     VARCHAR(255)       COMMENT 'researved.  Sourced from: PROGREC.TXT.TF_USER_DATA_3.  2008.08.20: A value of NULL or spaces should be transformed to "N/A"',
   DTM_CREATED                     TIMESTAMP        COMMENT 'date/time the row was created into the table.  Sourced from: SYSDATE',
   DTM_LAST_UPDATED                TIMESTAMP        COMMENT '',
   SOURCED_FROM_SYSTEM             VARCHAR(31)        COMMENT '',
   LAST_UPDATED_BY_SYSTEM          VARCHAR(31)        COMMENT '',
   TGT_DTM_CREATED                 TIMESTAMP        COMMENT '',
   TGT_DTM_LAST_UPDATED            TIMESTAMP        COMMENT '',
   ROOTID                          VARCHAR(255)       COMMENT '',
   VERSIONID                       VARCHAR(255)       COMMENT '',
   CONNECTORID                     VARCHAR(255)       COMMENT '',
   SEASONID                        VARCHAR(255)       COMMENT '',
   SERIESID                        VARCHAR(255)       COMMENT '',
   EI_SEASON                       VARCHAR(255)       COMMENT '',
   EI_NUMBER                       VARCHAR(255)       COMMENT '',
   EI_EPISODES                     VARCHAR(255)       COMMENT ''
)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################