
CREATE OR REPLACE VIEW UNIQUE_VISITOR_AGG AS SELECT * FROM MDRMGR.UNIQUE_VISITOR_AGG;

CREATE OR REPLACE VIEW MKTSAS_UNIQUE_VISITOR_AGG AS SELECT * FROM MDRMGR.UNIQUE_VISITOR_AGG;

GRANT SELECT ON MDRUSR.MKTSAS_UNIQUE_VISITOR_AGG TO MDR_SELECT_DEV;