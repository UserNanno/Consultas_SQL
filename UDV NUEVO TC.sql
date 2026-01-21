BEGIN
  DECLARE v_sql STRING;
  SET v_sql = 'SELECT 1 AS x';
  EXECUTE IMMEDIATE v_sql;
END;
