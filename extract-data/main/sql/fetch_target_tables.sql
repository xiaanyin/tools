SELECT OWNER,
  OBJECT_NAME
FROM DBA_OBJECTS
WHERE OWNER = ::SCHEMA::
AND OBJECT_TYPE = 'TABLE'
ORDER BY 1, 2
