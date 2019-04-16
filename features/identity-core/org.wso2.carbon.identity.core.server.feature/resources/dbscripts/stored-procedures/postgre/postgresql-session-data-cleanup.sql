CREATE OR REPLACE FUNCTION cleanup_session_data()
  RETURNS VOID AS $$
DECLARE

  -- ------------------------------------------
  -- DECLARE VARIABLES
  -- ------------------------------------------

  deletedsessions                INT = 0;
  deletedstoreoperations         INT = 0;
  deleteddeleteoperations        INT = 0;
  -- SET IF TRACE LOGGING IS ENABLED [DEFAULT : FALSE]
  tracingenabled                 BOOLEAN = TRUE;
  -- Sleep time in seconds.
  sleeptime                      DOUBLE PRECISION = 2;
  batchsize                      INT = 5000;
  -- This defines the number of entries from IDN_AUTH_SESSION_STORE that are taken into a SNAPSHOT
  chunklimit                     INT = 1000000;
  cleanupcompleted               BOOLEAN = FALSE;
  -- Session data older than 14 days will be removed.
  sessioncleanuptime             TIMESTAMP = sys_extract_utc(current_timestamp) - 14;
  -- Operational data older than 12 h will be removed.
  operationcleanuptime           TIMESTAMP = sys_extract_utc(current_timestamp) - (12 / 24);
  sessioncleanupcount            INT = 1;
  operationcleanupcount          INT = 1;
  sessioncleanuptemptablecount   INT = 1;
  operationcleanuptemptablecount INT = 1;
  rowcount                       INT = 0;
  current_schema                 VARCHAR(20);

BEGIN

  -- ------------------------------------------
  -- REMOVE SESSION DATA
  -- ------------------------------------------
  IF tracingenabled
  THEN
    RAISE NOTICE 'CLEANUP_SESSION_DATA() STARTED .... !';
  END IF;

  -- Cleanup any existing tables.

  DROP TABLE IF EXISTS TMP_IDN_AUTH_SESSION_STORE;
  DROP TABLE IF EXISTS TEMP_SESSION_BATCH;

  --
  WHILE sessioncleanuptemptablecount > 0 LOOP

    CREATE TABLE TMP_IDN_AUTH_SESSION_STORE AS
      (SELECT SESSION_ID
       FROM IDN_AUTH_SESSION_STORE
       WHERE TIME_CREATED < || DATE_TO_UNIX_TS(sessionCleanupTime) || AND ROWNUM <= chunkLimit);

    CREATE INDEX idn_auth_session_tmp_idx ON TMP_IDN_AUTH_SESSION_STORE (SESSION_ID);

    SELECT COUNT(1)
    FROM TMP_IDN_AUTH_SESSION_STORE
    INTO sessioncleanuptemptablecount;

    IF tracingenabled
    THEN
      RAISE NOTICE 'TEMPORARY SESSION CLEANUP TASK SNAPSHOT TABLE CREATED: %', sessioncleanuptemptablecount;
    END IF;

    sessioncleanupcount := 1;
    WHILE sessioncleanupcount > 0 LOOP

      CREATE TABLE TEMP_SESSION_BATCH AS (SELECT SESSION_ID
                                          FROM TMP_IDN_AUTH_SESSION_STORE
                                          WHERE rownum <= ||batchSize);

      DELETE FROM idn_auth_session_store
      WHERE SESSION_ID IN (SELECT "SESSION_ID"
                           FROM temp_session_batch);

      GET DIAGNOSTICS sessioncleanupcount := ROW_COUNT;

      IF tracingenabled
      THEN
        RAISE NOTICE 'DELETED SESSION COUNT: %', sessioncleanupcount;
      END IF;

      DELETE FROM TMP_IDN_AUTH_SESSION_STORE
      WHERE SESSION_ID IN (SELECT SESSION_ID
                           FROM temp_session_batch);

      IF tracingenabled
      THEN
        RAISE NOTICE 'END CLEANING UP IDS FROM TEMP SESSION DATA SNAPSHOT TABLE...!!';
      END IF;

      DROP TABLE TEMP_SESSION_BATCH;

      IF tracingenabled
      THEN
        deletedsessions := deletedsessions + sessioncleanupcount;
        RAISE NOTICE 'REMOVED SESSIONS: % NO OF DELETED ENTRIES', deletedsessions;
      END IF;

      -- Sleep for some time letting other threads to run.
      PERFORM pg_sleep(sleeptime);
    END LOOP;

    -- DROP THE CHUNK TO MOVE ON TO THE NEXT CHUNK IN THE SNAPSHOT TABLE.
    DROP TABLE TMP_IDN_AUTH_SESSION_STORE;
  END LOOP;

  IF tracingenabled
  THEN
    RAISE NOTICE 'SESSION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE: % COMPLETED_TIMESTAMP: %', deletedsessions,
    current_timestamp;
    RAISE NOTICE 'SESSION_CLEANUP_TASK ENDED .... !';
  END IF;

  -- --------------------------------------------
  -- REMOVE OPERATIONAL DATA
  -- --------------------------------------------

  IF tracingenabled
  THEN
    RAISE NOTICE 'OPERATION_CLEANUP_TASK STARTED AT: %', current_timestamp;
  END IF;

  DROP TABLE TMP_IDN_AUTH_SESSION_STORE;
  DROP TABLE TEMP_SESSION_BATCH;

  WHILE operationcleanuptemptablecount > 0 LOOP
    CREATE TABLE TMP_IDN_AUTH_SESSION_STORE AS
      SELECT
        SESSION_ID,
        SESSION_TYPE
      FROM IDN_AUTH_SESSION_STORE
      WHERE OPERATION = "DELETE" AND TIME_CREATED < DATE_TO_UNIX_TS(operationCleanupTime) ||
            AND ROWNUM < chunkLimit;
    CREATE INDEX idn_auth_session_tmp_idx
      ON TMP_IDN_AUTH_SESSION_STORE (SESSION_ID);
    SELECT COUNT(1)
    FROM TMP_IDN_AUTH_SESSION_STORE
    INTO operationcleanuptemptablecount;

    IF tracingenabled
    THEN
      RAISE NOTICE 'TEMPORARY OPERATION CLEANUP SNAPSHOT TABLE CREATED %', operationcleanuptemptablecount;
    END IF;

    operationcleanupcount := 1;
    WHILE (operationcleanupcount > 0) LOOP
      CREATE TABLE TEMP_SESSION_BATCH AS
        SELECT
          SESSION_ID,
          SESSION_TYPE
        FROM TMP_IDN_AUTH_SESSION_STORE
        WHERE ROWNUM < batchSize;

      DELETE FROM idn_auth_session_store
      WHERE SESSION_ID IN (SELECT SESSION_ID FROM idn_auth_session_store)
            AND SESSION_TYPE IN (SELECT SESSION_TYPE FROM TMP_IDN_AUTH_SESSION_STORE) ;

      GET DIAGNOSTICS operationcleanupcount := ROW_COUNT;

      IF tracingenabled
      THEN
        RAISE NOTICE 'DELETED STORE OPERATIONS COUNT: %', operationcleanupcount;
      END IF;

      IF (tracingenabled)
      THEN
        deleteddeleteoperations := operationcleanupcount + deleteddeleteoperations;
        RAISE NOTICE 'REMOVED DELETE OPERATION RECORDS: %', deleteddeleteoperations;
      END IF;

      DELETE FROM TMP_IDN_AUTH_SESSION_STORE WHERE SESSION_ID IN (SELECT SESSION_ID FROM TEMP_SESSION_BATCH);

      IF tracingenabled
      THEN
        RAISE NOTICE 'ENDED CLEANING UP IDS FROM TEMP OPERATIONAL DATA SNAPSHOT TABLE...!!';
      END IF;

      IF (tracingenabled)
      THEN
        deletedstoreoperations := operationcleanupcount + deletedstoreoperations;
        RAISE NOTICE 'REMOVED STORE OPERATION RECORDS: %', deletedstoreoperations;
      END IF;

      DROP TABLE TEMP_SESSION_BATCH;
      -- Sleep for some time letting other threads to run.
      PERFORM pg_sleep(sleeptime);
    END LOOP;

    DROP TABLE TMP_IDN_AUTH_SESSION_STORE;
  END LOOP;

  IF (tracingenabled)
  THEN
    deletedstoreoperations := operationcleanupcount + deletedstoreoperations;
    RAISE NOTICE 'DELETE OPERATION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE: %', deleteddeleteoperations;
    RAISE NOTICE 'FLAG SET TO INDICATE END OF CLEAN UP TASK...!!'')';
    RAISE NOTICE 'CLEANUP_SESSION_DATA() ENDED .... !'')';
  END IF;

END;
$$
LANGUAGE plpgsql;