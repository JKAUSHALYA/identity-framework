CREATE OR REPLACE PROCEDURE cleanup_session_data IS

  -- ------------------------------------------
  -- DECLARE VARIABLES
  -- ------------------------------------------

  deletedsessions                INT := 0;
  deletedstoreoperations         INT := 0;
  deleteddeleteoperations        INT := 0;
  -- SET IF TRACE LOGGING IS ENABLED [DEFAULT : FALSE]
  tracingenabled                 BOOLEAN := TRUE;
  -- Sleep time in seconds.
  sleeptime                      FLOAT := 2;
  batchsize                      INT := 5000;
  -- This defines the number of entries from IDN_AUTH_SESSION_STORE that are taken into a SNAPSHOT
  chunklimit                     INT := 1000000;
  cleanupcompleted               BOOLEAN := FALSE;
  -- Session data older than 14 days will be removed.
  sessioncleanuptime             TIMESTAMP := sys_extract_utc(systimestamp) - 14;
  -- Operational data older than 12 h will be removed.
  operationcleanuptime           TIMESTAMP := sys_extract_utc(systimestamp) - (12/24);
  sessioncleanupcount            INT := 1;
  operationcleanupcount          INT := 1;
  sessioncleanuptemptablecount   INT := 1;
  operationcleanuptemptablecount INT := 1;
  rowcount                       INT := 0;
  current_schema                 VARCHAR(20);

  BEGIN
    -- Create the log table.
    SELECT sys_context('USERENV', 'CURRENT_SCHEMA')
    INTO current_schema
    FROM
      dual;

    SELECT COUNT(1)
    INTO rowcount
    FROM
      all_tables
    WHERE
      owner = current_schema
      AND table_name = upper('LOG_CLEANUP_SESSION_DATA');

    IF (rowcount = 1)
    THEN
      EXECUTE IMMEDIATE 'DROP TABLE LOG_CLEANUP_SESSION_DATA';
      COMMIT;
    END IF;
    EXECUTE IMMEDIATE 'CREATE TABLE WSO2_TOKEN_CLEANUP_SP_LOG (
      TIMESTAMP VARCHAR(250),
      LOG       VARCHAR(250)
    ) NOLOGGING';
    COMMIT;

    -- ------------------------------------------
    -- REMOVE SESSION DATA
    -- ------------------------------------------
    IF tracingenabled
    THEN
      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
      VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''CLEANUP_SESSION_DATA() STARTED .... !'')';
      COMMIT;
    END IF;

    -- Cleanup any existing tables.
    SELECT COUNT(1)
    INTO rowcount
    FROM
      all_tables
    WHERE
      owner = current_schema
      AND table_name = upper('TMP_IDN_AUTH_SESSION_STORE');

    IF (rowcount = 1)
    THEN
      EXECUTE IMMEDIATE 'DROP TABLE TMP_IDN_AUTH_SESSION_STORE';
      COMMIT;
    END IF;
    SELECT COUNT(1)
    INTO rowcount
    FROM
      all_tables
    WHERE
      owner = current_schema
      AND table_name = upper('TEMP_SESSION_BATCH');

    IF (rowcount = 1)
    THEN
      EXECUTE IMMEDIATE 'DROP TABLE TEMP_SESSION_BATCH';
      COMMIT;
    END IF;

    --
    WHILE sessioncleanuptemptablecount > 0 LOOP

      EXECUTE IMMEDIATE 'CREATE TABLE TMP_IDN_AUTH_SESSION_STORE AS
        (SELECT SESSION_ID
        FROM IDN_AUTH_SESSION_STORE
        WHERE TIME_CREATED < ' || DATE_TO_UNIX_TS(sessionCleanupTime) || ' AND ROWNUM <= chunkLimit)';
      COMMIT;

      EXECUTE IMMEDIATE 'CREATE INDEX idn_auth_session_tmp_idx ON TMP_IDN_AUTH_SESSION_STORE (SESSION_ID)';
      COMMIT;

      EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM TMP_IDN_AUTH_SESSION_STORE' INTO sessioncleanuptemptablecount;

      IF tracingenabled
      THEN
        EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
        VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''TEMPORARY SESSION CLEANUP TASK SNAPSHOT TABLE CREATED...!!'
                          || sessioncleanuptemptablecount
                          || ')';
        COMMIT;
      END IF;
      sessioncleanupcount := 1;
      WHILE sessioncleanupcount > 0 LOOP

        EXECUTE IMMEDIATE 'CREATE TABLE TEMP_SESSION_BATCH AS (SELECT SESSION_ID
                                                               FROM TMP_IDN_AUTH_SESSION_STORE
                                                               WHERE rownum <= ' || batchSize || ')';
        COMMIT;

        EXECUTE IMMEDIATE 'DELETE (
          SELECT *
          FROM
            idn_auth_session_store a
            INNER JOIN temp_session_batch b ON a.session_id = b.session_id
        )';
        COMMIT;

        sessioncleanupcount := SQL%ROWCOUNT;

        IF tracingenabled
        THEN
          EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''DELETED SESSION COUNT...!!'
                            || sessioncleanupcount
                            || ')';
          COMMIT;
        END IF;

        EXECUTE IMMEDIATE 'DELETE (
          SELECT *
          FROM
            TMP_IDN_AUTH_SESSION_STORE a
            INNER JOIN temp_session_batch b ON a.session_id = b.session_id
        )';
        COMMIT;

        IF tracingenabled
        THEN
          EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''),
                  ''END CLEANING UP IDS FROM TEMP SESSION DATA SNAPSHOT TABLE...!!'')';
          COMMIT;
        END IF;

        EXECUTE IMMEDIATE 'DROP TABLE TEMP_SESSION_BATCH';

        IF tracingenabled THEN
          deletedsessions := deletedsessions + sessioncleanupcount;
          EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
            VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), '
                            || 'REMOVED SESSIONS: '
                            || deletedsessions
                            || ' NO OF DELETED ENTRIES'
                            || ')';
          COMMIT;
        END IF;

        -- Sleep for some time letting other threads to run.
        dbms_lock.sleep(sleeptime);
      END LOOP;

      -- DROP THE CHUNK TO MOVE ON TO THE NEXT CHUNK IN THE SNAPSHOT TABLE.
      EXECUTE IMMEDIATE 'DROP TABLE TMP_IDN_AUTH_SESSION_STORE';
      COMMIT;
    END LOOP;

    IF tracingenabled THEN
      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
      VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), '
                        || 'SESSION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE. '
                        || deletedsessions
                        || 'TOTAL NO OF DELETED ENTRIES'
                        || systimestamp
                        || 'COMPLETED_TIMESTAMP'
                        || ')';

      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
      VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''SESSION_CLEANUP_TASK ENDED .... !'')';
      COMMIT;
    END IF;

    -- --------------------------------------------
    -- REMOVE OPERATIONAL DATA
    -- --------------------------------------------

    IF tracingenabled THEN
      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
      VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), '
                        || 'OPERATION_CLEANUP_TASK STARTED .... !'
                        || systimestamp
                        || 'STARTING TIMESTAMP'
                        || ')';
      COMMIT;
    END IF;

    EXECUTE IMMEDIATE 'DROP TABLE TMP_IDN_AUTH_SESSION_STORE';
    EXECUTE IMMEDIATE 'DROP TABLE TEMP_SESSION_BATCH';
    COMMIT;

    WHILE operationcleanuptemptablecount > 0 LOOP
      EXECUTE IMMEDIATE 'CREATE TABLE TMP_IDN_AUTH_SESSION_STORE AS
        SELECT
          SESSION_ID,
          SESSION_TYPE
        FROM IDN_AUTH_SESSION_STORE
        WHERE OPERATION = "DELETE" AND TIME_CREATED < ' || DATE_TO_UNIX_TS(operationCleanupTime) || 'AND ROWNUM < chunkLimit';
      EXECUTE IMMEDIATE 'CREATE INDEX idn_auth_session_tmp_idx ON TMP_IDN_AUTH_SESSION_STORE (SESSION_ID)';
      COMMIT;

      EXECUTE IMMEDIATE 'SELECT COUNT(1) FROM TMP_IDN_AUTH_SESSION_STORE' INTO operationcleanuptemptablecount;
      COMMIT;

      IF tracingenabled THEN
        EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
        VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''TEMPORARY OPERATION CLEANUP SNAPSHOT TABLE CREATED...!!'
                          || operationcleanuptemptablecount
                          || ')';
        COMMIT;
      END IF;

      operationcleanupcount := 1;
      WHILE (operationcleanupcount > 0) LOOP
        EXECUTE IMMEDIATE 'CREATE TABLE TEMP_SESSION_BATCH AS
          SELECT
            SESSION_ID,
            SESSION_TYPE
          FROM TMP_IDN_AUTH_SESSION_STORE
          WHERE ROWNUM < batchSize';

        EXECUTE IMMEDIATE 'DELETE(
          SELECT *
          FROM
            idn_auth_session_store a
            INNER JOIN temp_session_batch b ON a.session_id = b.session_id
                                               AND a.session_type = b.session_type
        )';
        COMMIT;

        operationcleanupcount := SQL%ROWCOUNT;

        IF tracingenabled THEN
          EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''DELETED STORE OPERATIONS COUNT...!! '
                            || operationcleanupcount
                            || ')';
          COMMIT;
        END IF;

        IF (tracingenabled)
        THEN
          deleteddeleteoperations := operationcleanupcount + deleteddeleteoperations;
          EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''REMOVED DELETE OPERATION RECORDS: '
                            || deleteddeleteoperations
                            || ')';
          COMMIT;
        END IF;

        EXECUTE IMMEDIATE 'DELETE (
          SELECT *
          FROM
            TMP_IDN_AUTH_SESSION_STORE a
            INNER JOIN temp_session_batch b ON a.session_id = b.session_id
        )';

        IF tracingenabled
        THEN
          EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''),
                  ''ENDED CLEANING UP IDS FROM TEMP OPERATIONAL DATA SNAPSHOT TABLE...!!'')';
          COMMIT;
        END IF;

        IF (tracingenabled) THEN
          deletedstoreoperations := operationcleanupcount + deletedstoreoperations;
          EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''REMOVED STORE OPERATION RECORDS: '
                            || deletedstoreoperations
                            || ')';
        END IF;

        EXECUTE IMMEDIATE 'DROP TABLE TEMP_SESSION_BATCH';
        COMMIT;
        -- Sleep for some time letting other threads to run.
        dbms_lock.sleep(sleeptime);
      END LOOP;

      EXECUTE IMMEDIATE 'DROP TABLE TMP_IDN_AUTH_SESSION_STORE';
      COMMIT;
    END LOOP;

    IF (tracingenabled) THEN
      deletedstoreoperations := operationcleanupcount + deletedstoreoperations;
      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
      VALUES
        (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''FLAG SET TO INDICATE END OF CLEAN UP TASK...!!'')';
      COMMIT;
    END IF;

    IF (tracingenabled)
    THEN
      deletedstoreoperations := operationcleanupcount + deletedstoreoperations;
      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''STORE OPERATION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE: '
                        || deletedstoreoperations
                        || ')';
      COMMIT;
    END IF;

    IF (tracingenabled)
    THEN
      deletedstoreoperations := operationcleanupcount + deletedstoreoperations;
      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
          VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''DELETE OPERATION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE: '
                        || deleteddeleteoperations
                        || ')';
      COMMIT;
    END IF;

    IF (tracingenabled)
    THEN
      deletedstoreoperations := operationcleanupcount + deletedstoreoperations;
      EXECUTE IMMEDIATE 'INSERT INTO WSO2_TOKEN_CLEANUP_SP_LOG (TIMESTAMP, LOG)
      VALUES (TO_CHAR(SYSTIMESTAMP, ''DD.MM.YYYY HH24:MI:SS:FF4''), ''CLEANUP_SESSION_DATA() ENDED .... !'')';
      COMMIT;
    END IF;

  END;