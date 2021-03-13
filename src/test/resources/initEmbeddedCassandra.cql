/* Init keyspaces and tables for MetadataResultSetsUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace;
CREATE KEYSPACE "test_keyspace" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

DROP KEYSPACE IF EXISTS test_keyspace2;
CREATE KEYSPACE "test_keyspace2" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace;
CREATE COLUMNFAMILY cf_test1 (
keyname text PRIMARY KEY,
t1bValue boolean,
t1iValue int)
WITH comment = 'First table in the keyspace';

CREATE COLUMNFAMILY cf_test2 (
keyname text PRIMARY KEY,
t2bValue boolean,
t2iValue int)
WITH comment = 'Second table in the keyspace';

USE test_keyspace2;
CREATE COLUMNFAMILY cf_test1 (
keyname text PRIMARY KEY,
t1bValue boolean,
t1iValue int)
WITH comment = 'First table in the keyspace';

CREATE COLUMNFAMILY cf_test2 (
keyname text PRIMARY KEY,
t2bValue boolean,
t2iValue int)
WITH comment = 'Second table in the keyspace';

/* Init keyspace and tables for PooledUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace_pool;
CREATE KEYSPACE "test_keyspace_pool" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace_pool;
CREATE COLUMNFAMILY pooled_test (
somekey text PRIMARY KEY,
someInt int);

UPDATE pooled_test SET someInt = 1 WHERE somekey = 'world';

/* Init keyspace and tables for CollectionsUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace_coll;
CREATE KEYSPACE "test_keyspace_coll" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace_coll;
CREATE TABLE collections_test (
keyValue int PRIMARY KEY,
listValue list<bigint>,
mapValue map<double, boolean>,
setValue set<text>);

INSERT INTO collections_test (keyValue, listValue) VALUES(1, [1, 3, 12345]);
UPDATE collections_test SET setValue = {'red', 'white', 'blue'} WHERE keyValue = 1;
UPDATE collections_test SET mapValue = {2.0: true, 4.0: false, 6.0 : true} WHERE keyValue = 1;

/* Init keyspace and tables for BatchStatementsUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace_batch;
CREATE KEYSPACE "test_keyspace_batch" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace_batch;
CREATE TABLE collections_test (
keyValue int PRIMARY KEY,
listValue list<bigint>,
mapValue map<double, boolean>,
setValue set<text>);

/* Init keyspace and tables for JdbcRegressionUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace3;
CREATE KEYSPACE "test_keyspace3" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace3;
CREATE COLUMNFAMILY regressions_test (
keyname text PRIMARY KEY,
bValue boolean,
iValue int);

CREATE INDEX ON test_keyspace3.regressions_test (iValue);
