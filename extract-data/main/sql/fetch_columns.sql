SELECT COLS.TABLE_NAME,
  COLS.COLUMN_NAME,
  CONS.CONSTRAINT_TYPE,
  COAT.DATA_TYPE
FROM ALL_CONSTRAINTS CONS.
  ALL_CONS_COLUMNS COLS,
  ALL_TAB_COLUMNS COAT
WHERE CONS.OWNER = ::SCHEMA::
AND CONS.CONSTRAINT_NAME = COLS.CONSTRAINT_NAME
AND CONS.OWNER = COLS.OWNER
AND CONS.OWNER = COAT.OWNER
AND COLS.TABLE_NAME = COAT.TABLE_NAME
AND COLS.COLUMN_NAME = COAT.COLUMN_NAME
ORDER BY CONS.OWNER, COLS.TABLE_NAME, COLS.POSITION
