BEGIN
  DECLARE v_sql STRING;

  SET v_sql = 'SELECT (''41'') AS x';
  EXECUTE IMMEDIATE v_sql;
END;
