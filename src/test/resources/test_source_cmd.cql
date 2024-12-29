INSERT INTO cf_test1 (keyname, t1bValue, t1iValue) VALUES('key100', true, 100);
INSERT INTO cf_test1 (keyname, t1bValue, t1iValue) VALUES('key101', false, 101);
INSERT INTO cf_test1 (keyname, t1bValue, t1iValue) VALUES('key102', true, 102);

/* The following statements return results but they are ignored not returned by the implementation of the this JDBC
driver when it is executed via a special CQL command */
SELECT keyname, t1bValue, t1iValue FROM cf_test1 WHERE keyname = 'key100';

/* The following statement will produce warning */
SELECT COUNT(keyname) FROM cf_test1;