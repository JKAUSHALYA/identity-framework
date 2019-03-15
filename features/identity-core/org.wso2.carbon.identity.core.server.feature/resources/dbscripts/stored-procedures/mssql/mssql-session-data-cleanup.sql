CREATE PROCEDURE CLEANUP_SESSION_DATA
AS
  BEGIN
    SET NOCOUNT ON;

    -- ------------------------------------------
    -- DECLARE VARIABLES
    -- ------------------------------------------
    DECLARE @deletedSessions INT;
    DECLARE @deletedStoreOperations INT;
    DECLARE @deletedDeleteOperations INT;
    DECLARE @sessionCleanupCount INT;
    DECLARE @operationCleanupCount INT;
    DECLARE @tracingEnabled BIT;
    DECLARE @sleepTime INT;
    DECLARE @batchSize INT;
    DECLARE @chunkLimit INT;

    DECLARE @sessionCleanUpTempTableCount INT;
    DECLARE @operationCleanUpTempTableCount INT;
    DECLARE @cleanUpCompleted BIT;

    DECLARE @autocommit BIT;

    DECLARE @sessionCleanupTime BIGINT;
    DECLARE @operationCleanupTime BIGINT;

    -- ------------------------------------------
    -- CONFIGURABLE VARIABLES
    -- ------------------------------------------

    SET @batchSize = 5000;
    -- This defines the number of entries from IDN_AUTH_SESSION_STORE that are taken into a SNAPSHOT
    SET @chunkLimit = 1000000;
    SET @deletedSessions = 0;
    SET @deletedStoreOperations = 0;
    SET @deletedDeleteOperations = 0;
    SET @sessionCleanupCount = 1;
    SET @operationCleanupCount = 1;
    SET @tracingEnabled = 'TRUE'; -- SET IF TRACE LOGGING IS ENABLED [DEFAULT : FALSE]
    SET @sleepTime = 2; -- Sleep time in seconds.
    SET @autocommit = 0;

    SET @sessionCleanUpTempTableCount = 1;
    SET @operationCleanUpTempTableCount = 1;
    SET @cleanUpCompleted = 'FALSE';

    -- Session data older than 14 days will be removed.
    SET @sessionCleanupTime = DATEDIFF(SECOND,{d '1970-01-01'}, DATEADD(day, -14, GETUTCDATE()));
    -- Operational data older than 12 h will be removed.
    SET @operationCleanupTime = DATEDIFF(SECOND,{d '1970-01-01'}, DATEADD(hour, -12, GETUTCDATE()));

    -- ------------------------------------------
    -- REMOVE SESSION DATA
    -- ------------------------------------------

    SELECT
      'CLEANUP_SESSION_DATA() STARTED .... !' AS 'INFO LOG',
      GETDATE()                               AS 'STARTING TIMESTAMP';

    -- CLEANUP ANY EXISTING TEMP TABLES
    DROP TABLE IF EXISTS IDN_AUTH_SESSION_STORE_TMP;
    DROP TABLE IF EXISTS TEMP_SESSION_BATCH;

    -- RUN UNTILL
    WHILE (@sessionCleanUpTempTableCount > 0) BEGIN

      SELECT TOP (@chunkLimit) SESSION_ID INTO IDN_AUTH_SESSION_STORE_TMP FROM IDN_AUTH_SESSION_STORE
      WHERE TIME_CREATED < @sessionCleanupTime;
      CREATE INDEX idn_auth_session_tmp_idx
        ON IDN_AUTH_SESSION_STORE_TMP (SESSION_ID);

      SELECT @sessionCleanUpTempTableCount = COUNT(1)
      FROM IDN_AUTH_SESSION_STORE_TMP;
      SELECT
        'TEMPORARY SESSION CLEANUP TASK SNAPSHOT TABLE CREATED...!!' AS 'INFO LOG',
        @sessionCleanUpTempTableCount;

      SET @sessionCleanupCount = 1;
      WHILE (@sessionCleanupCount > 0) BEGIN

        SELECT TOP (@BATCHSIZE) SESSION_ID INTO TEMP_SESSION_BATCH FROM IDN_AUTH_SESSION_STORE_TMP;

        DELETE A
        FROM IDN_AUTH_SESSION_STORE AS A
          INNER JOIN TEMP_SESSION_BATCH AS B ON
                                               A.SESSION_ID = B.SESSION_ID;
        SET @sessionCleanupCount = @@ROWCOUNT

        SELECT
          'DELETED SESSION COUNT...!!' AS 'INFO LOG',
          @sessionCleanupCount;

        DELETE A
        FROM IDN_AUTH_SESSION_STORE_TMP AS A
          INNER JOIN TEMP_SESSION_BATCH AS B
            ON A.SESSION_ID = B.SESSION_ID;

        SELECT 'END CLEANING UP IDS FROM TEMP SESSION DATA SNAPSHOT TABLE...!!' AS 'INFO LOG';

        DROP TABLE TEMP_SESSION_BATCH;

        IF (@tracingEnabled = 1)
          BEGIN SET
          @deletedSessions = @deletedSessions + @sessionCleanupCount;
            SELECT
              'REMOVED SESSIONS: ' AS 'INFO LOG',
              @deletedSessions     AS 'NO OF DELETED ENTRIES',
              GETDATE()            AS 'TIMESTAMP';
          END
        WAITFOR DELAY @sleepTime;
        -- Sleep for some time letting other threads to run.
      END;

      -- DROP THE CHUNK TO MOVE ON TO THE NEXT CHUNK IN THE SNAPSHOT TABLE.
      DROP TABLE IF EXISTS IDN_AUTH_SESSION_STORE_TMP;

    END;

    IF (@tracingEnabled = 1)
      BEGIN
        SELECT
          'SESSION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE: ' AS 'INFO LOG',
          @deletedSessions                                        AS 'TOTAL NO OF DELETED ENTRIES',
          GETDATE()                                               AS 'COMPLETED_TIMESTAMP';
      END

    SELECT 'SESSION_CLEANUP_TASK ENDED .... !' AS 'INFO LOG';

    -- --------------------------------------------
    -- REMOVE OPERATIONAL DATA
    -- --------------------------------------------

    SELECT
      'OPERATION_CLEANUP_TASK STARTED .... !' AS 'INFO LOG',
      GETDATE()                               AS 'STARTING TIMESTAMP';
    SELECT 'BATCH DELETE STARTED .... ' AS 'INFO LOG';

    DROP TABLE IF EXISTS IDN_AUTH_SESSION_STORE_TMP;
    DROP TABLE IF EXISTS TEMP_SESSION_BATCH;

    WHILE (@operationCleanUpTempTableCount > 0) BEGIN

      SELECT TOP (@chunkLimit) SESSION_ID, SESSION_TYPE INTO IDN_AUTH_SESSION_STORE_TMP
      FROM IDN_AUTH_SESSION_STORE
      WHERE OPERATION = 'DELETE' AND
            TIME_CREATED < @operationCleanupTime;
      CREATE INDEX idn_auth_session_tmp_idx
        ON IDN_AUTH_SESSION_STORE_TMP (SESSION_ID);

      SELECT @operationCleanUpTempTableCount = COUNT(1)
      FROM IDN_AUTH_SESSION_STORE_TMP;
      SELECT
        'TEMPORARY OPERATION CLEANUP SNAPSHOT TABLE CREATED...!!' AS 'INFO LOG',
        @operationCleanUpTempTableCount;

      SET @operationCleanupCount = 1;
      WHILE (@operationCleanupCount > 0) BEGIN

        SELECT TOP(@BATCHSIZE) SESSION_ID, SESSION_TYPE INTO TEMP_SESSION_BATCH FROM IDN_AUTH_SESSION_STORE_TMP;

        DELETE A
        FROM IDN_AUTH_SESSION_STORE AS A
          INNER JOIN TEMP_SESSION_BATCH AS B
            ON A.SESSION_ID = B.SESSION_ID
               AND A.SESSION_TYPE = B.SESSION_TYPE;
        SELECT @operationCleanupCount = @@ROWCOUNT;

        SELECT
          'DELETED STORE OPERATIONS COUNT...!!' AS 'INFO LOG',
          @operationCleanupCount;

        IF (@tracingEnabled = 1)
          BEGIN
            SET @deletedDeleteOperations = @operationCleanupCount + @deletedDeleteOperations;
            SELECT
              'REMOVED DELETE OPERATION RECORDS: ' AS 'INFO LOG',
              @deletedDeleteOperations             AS 'NO OF DELETED DELETE ENTRIES',
              GETDATE()                            AS 'TIMESTAMP';
          END

        DELETE A
        FROM IDN_AUTH_SESSION_STORE_TMP AS A
          INNER JOIN TEMP_SESSION_BATCH AS B
            ON A.SESSION_ID = B.SESSION_ID;

        SELECT 'ENDED CLEANING UP IDS FROM TEMP OPERATIONAL DATA SNAPSHOT TABLE...!!' AS 'INFO LOG';

        IF (@tracingEnabled = 1)
          BEGIN
            SET @deletedStoreOperations = @operationCleanupCount + @deletedStoreOperations;
            SELECT
              'REMOVED STORE OPERATION RECORDS: ' AS 'INFO LOG',
              @deletedStoreOperations             AS 'NO OF DELETED STORE ENTRIES',
              GETDATE()                           AS 'TIMESTAMP';
          END

        DROP TABLE TEMP_SESSION_BATCH;

        WAITFOR DELAY @sleepTime; -- Sleep for some time letting other threads to run.
      END;

      DROP TABLE IF EXISTS IDN_AUTH_SESSION_STORE_TMP;

    END;

    SELECT 'FLAG SET TO INDICATE END OF CLEAN UP TASK...!!' AS 'INFO LOG';

    IF (@tracingEnabled = 1)
      BEGIN
        SELECT
          'STORE OPERATION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE: ' AS 'INFO LOG',
          @deletedStoreOperations                                         AS 'TOTAL NO OF DELETED STORE ENTRIES',
          GETDATE()                                                       AS 'COMPLETED_TIMESTAMP';
        SELECT
          'DELETE OPERATION RECORDS REMOVED FROM IDN_AUTH_SESSION_STORE: ' AS 'INFO LOG',
          @deletedDeleteOperations                                         AS 'TOTAL NO OF DELETED DELETE ENTRIES',
          GETDATE()                                                        AS 'COMPLETED_TIMESTAMP';
      END

    SELECT
      'CLEANUP_SESSION_DATA() ENDED .... !' AS 'INFO LOG',
      GETDATE()                             AS 'ENDING TIMESTAMP';
  END
