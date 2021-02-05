-- USE SCHEMA BI.ALERTS;
USE SCHEMA BI_TEST.ALERTS;
--
-- Spend tracking alert updater SP
--
-- CALL ALERTS.BUYSIDE_ACCOUNT_DATA_TRACK_ALERT_UPDATER ();
CREATE OR REPLACE PROCEDURE BUYSIDE_ACCOUNT_DATA_TRACK_ALERT_UPDATER ()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
var snowRet = '', snowSql = `
-- under github https://github.com/Openmail/BusinessIntelligence/tree/master/S1-Data-Incomplete-Detector/scripts/buyside_account_data_track_alert_updater.sql
UPDATE BUYSIDE_ACCOUNT_DATA_TRACKING D
SET SPEND_FOUND_TIME = COALESCE(D.SPEND_FOUND_TIME, CURRENT_TIMESTAMP)
	,DIFF_AMOUNT = ROUND(S.SPEND - D.SPEND_FORECAST, 2)
	,DIFF_PERCENTAGE = ROUND(100*((S.SPEND  - D.SPEND_FORECAST)/NULLIF(D.SPEND_FORECAST,0)),2)
	,SPEND_ACTUAL = S.SPEND
	,ALERT_STATUS = FALSE
FROM (
      -- Accounts having data entered
      SELECT A.DATA_DATE
        --,A.PRODUCT_LINE_ID
        --,A.NETWORK_NAME_ID
        ,A.BI_ACCOUNT_ID
        ,SUM(A.SPEND) SPEND
      FROM BI.ACCOUNT_DATA.BUYSIDE_ACCOUNT_DATA_DAILY_PRESENTATION A
      JOIN BI.COMMON.ACCOUNT_METADATA_MAPPINGS B
      ON A.BI_ACCOUNT_ID = B.BI_ACCOUNT_ID
      WHERE A.DATA_DATE >= CURRENT_DATE()-7 -- scope to recent X days of data
      AND B.MISSING_SPEND_TRACKING_EXEMPTED != 1  -- exclude the exempted accounts
      GROUP BY 1,2--,3,4
	  HAVING SUM(A.SPEND) > 0
  ) S
WHERE D.DATA_DATE = S.DATA_DATE
  --AND D.PRODUCT_LINE_ID = S.PRODUCT_LINE_ID
  --AND D.NETWORK_NAME_ID = S.NETWORK_NAME_ID
  AND D.BI_ACCOUNT_ID = S.BI_ACCOUNT_ID
  AND D.ALERT_STATUS = TRUE
;`;

try {
  snowRet = snowflake.execute({ sqlText: snowSql });
  snowRet = "Success.";
  }
catch (err) {
  snowRet = "Failure: " + err
  }
finally {
  return snowRet.toString()
  }
$$
;
