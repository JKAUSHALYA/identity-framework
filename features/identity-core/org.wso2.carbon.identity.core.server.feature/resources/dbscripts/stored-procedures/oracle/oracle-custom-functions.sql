CREATE OR REPLACE FUNCTION DATE_TO_UNIX_TS(PDATE IN DATE)

  --- --------------------------------------------------------------------------------------------------------------
  --- This file contains custom functions created for the Oracle scripts. This should be run before compiling other
  --- stored procedures.
  --- --------------------------------------------------------------------------------------------------------------

  RETURN NUMBER IS
  L_UNIX_TS NUMBER;
  BEGIN
    L_UNIX_TS := (PDATE - DATE '1970-01-01') * 60 * 60 * 24;
    RETURN L_UNIX_TS;
  END;