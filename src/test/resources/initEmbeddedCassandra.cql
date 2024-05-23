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

INSERT INTO cf_test1 (keyname, t1bValue, t1iValue) VALUES('key1', true, 1);

CREATE COLUMNFAMILY cf_test2 (
keyname text PRIMARY KEY,
t2bValue boolean,
t2iValue int)
WITH comment = 'Second table in the keyspace';

CREATE COLUMNFAMILY cf_test3 (
keyname text,
t3bValue boolean,
t3iValue int,
PRIMARY KEY(keyname, t3iValue))
WITH comment = 'Third table in the keyspace';

CREATE TABLE tbl_test_timestamps (
keyname text PRIMARY KEY,
col_ts timestamp);

INSERT INTO tbl_test_timestamps (keyname, col_ts) VALUES('key1', '2023-11-01T11:30:25.789+0100');

CREATE TABLE tbl_test_blobs (
keyname text PRIMARY KEY,
col_blob blob);

INSERT INTO tbl_test_blobs (keyname, col_blob) VALUES('key1', textAsBlob('testValueAsClobInUtf8 with accents: Äîéè'));

CREATE TABLE tbl_test_texts (
keyname text PRIMARY KEY,
col_ascii ascii);

INSERT INTO tbl_test_texts (keyname, col_ascii) VALUES('key1', 'testValueAscii');

CREATE TYPE CustomType1 (
key1 int,
value1 text,
);

CREATE TYPE CustomType2 (
key2 tinyint,
value2 boolean,
);

CREATE TYPE type_in_different_ks (
t_key int,
t_value text,
);

CREATE FUNCTION function_test1 (var1 int, var2 text)
RETURNS NULL ON NULL INPUT
RETURNS int LANGUAGE java AS
'return 2 * var1;';

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

CREATE INDEX IF NOT EXISTS int_values_idx
ON cf_test2 (t2iValue);

CREATE TYPE type_in_different_ks (
t_key int,
t_value text,
);

/* Init keyspace and tables for PooledUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace_pool;
CREATE KEYSPACE "test_keyspace_pool" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace_pool;
CREATE COLUMNFAMILY pooled_test (
somekey text PRIMARY KEY,
someInt int);

UPDATE pooled_test SET someInt = 1 WHERE somekey = 'world';

/* Init keyspace, UDTs and tables for CollectionsUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace_coll;
CREATE KEYSPACE "test_keyspace_coll" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace_coll;
CREATE TABLE collections_test (
keyValue int PRIMARY KEY,
listValue list<bigint>,
mapValue map<double, boolean>,
setValue set<text>);

CREATE TYPE CustomType (
key int,
value1 text,
value2 boolean
);

CREATE TABLE frozen_test (
keyValue int PRIMARY KEY,
frozenList frozen<list<int>>,
frozenMap frozen<map<text, text>>,
frozenSet frozen<set<text>>,
innerTuple list<frozen<tuple<int, text, CustomType>>>,
innerUdt list<frozen<CustomType>>,
outerUdt frozen<CustomType>
);

INSERT INTO collections_test (keyValue, listValue) VALUES(1, [1, 3, 12345]);
UPDATE collections_test SET setValue = {'red', 'white', 'blue'} WHERE keyValue = 1;
UPDATE collections_test SET mapValue = {2.0: true, 4.0: false, 6.0: true} WHERE keyValue = 1;

INSERT INTO frozen_test (keyValue, frozenList, frozenMap, frozenSet, innerTuple, innerUdt, outerUdt)
VALUES(1, [1, 3, 123], {'k1': 'v1', 'k2': 'v2'}, {'i1', 'i2', 'i3'},
[(1, 'val1', {key: 10, value1: 'test', value2: true}), (2, 'val2', {key: 20, value1: 'test2', value2: false})],
[{key: 1, value1: 'test', value2: true}, {key: 2, value1: 'test2', value2: false}],
       {key: 1, value1: 'test', value2: true});

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

/* Init keyspaces and tables for PreparedStatementsUnitTest */
DROP KEYSPACE IF EXISTS test_prep_stmt;
CREATE KEYSPACE "test_prep_stmt" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_prep_stmt;
CREATE COLUMNFAMILY cf_test_ps (
keyname text PRIMARY KEY,
t1bValue boolean,
t1iValue int);
CREATE INDEX ON test_prep_stmt.cf_test_ps (t1bValue);

INSERT INTO cf_test_ps (keyname, t1bValue, t1iValue) VALUES('testRow1', true, 0);
INSERT INTO cf_test_ps (keyname, t1bValue, t1iValue) VALUES('testRow2', false, 0);
INSERT INTO cf_test_ps (keyname, t1bValue, t1iValue) VALUES('testRow3', true, 5);

CREATE TABLE test_ps_othertypes (
col_key text PRIMARY KEY,
col_tuple tuple<text, text>,
col_inet inet,
col_duration duration,
col_uuid uuid);

CREATE TABLE test_ps_blobs (
col_key text PRIMARY KEY,
col_blob blob);

CREATE TABLE test_ps_texts (
col_key text PRIMARY KEY,
col_text text);

CREATE TABLE test_ps_datetimes (
col_key text PRIMARY KEY,
col_time time,
col_date date,
col_ts timestamp);

/* Init keyspaces, UDTs and tables for JsonSupportUnitTest */
DROP KEYSPACE IF EXISTS test_json_support;
CREATE KEYSPACE "test_json_support" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_json_support;
CREATE TYPE CustomObject (
asciiValue ascii,
bigintValue bigint,
blobValue blob,
boolValue boolean,
dateValue date,
decimalValue decimal,
doubleValue double,
floatValue float,
inetValue inet,
intValue int,
listValue list<int>,
mapValue map<int, text>,
smallintValue smallint,
setValue set<int>,
textValue text,
timeValue time,
tsValue timestamp,
timeuuidValue timeuuid,
tinyintValue tinyint,
tupleValue tuple<int, text>,
uuidValue uuid,
varcharValue varchar,
varintValue varint
);

CREATE TABLE json_test (
key int PRIMARY KEY,
textValue text,
customObject frozen<CustomObject>);

INSERT INTO json_test (key, textValue, customObject) VALUES(1, 'firstRow', {
asciiValue: 'ascii text',
bigintValue: 12345678900000,
blobValue: textAsBlob('this is a blob'),
boolValue: true,
dateValue: '2023-03-25',
decimalValue: 18.97,
doubleValue: 2345.6,
floatValue: 21.3,
inetValue: '127.0.0.1',
intValue: 98,
listValue: [4, 6, 10],
mapValue: {3: 'three', 8: 'eight'},
smallintValue: 2,
setValue: {2, 3, 5},
textValue: 'simple text',
timeValue: '12:30:45.789',
tsValue: '2023-03-25 12:30:45.789',
timeuuidValue: now(),
tinyintValue: 12,
tupleValue: (5, 'five'),
uuidValue: uuid(),
varcharValue: 'varchar text',
varintValue: 4321
});

/* Init keyspace and tables for VectorsUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace_vect;
CREATE KEYSPACE "test_keyspace_vect" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace_vect;
CREATE TABLE vectors_test (
keyValue int PRIMARY KEY,
intsVector vector<int, 3>,
floatsVector vector<float, 4>);

CREATE TABLE IF NOT EXISTS pet_supply_vectors (
    product_id     TEXT PRIMARY KEY,
    product_name   TEXT,
    product_vector vector<float, 14>
);

/* Ni similarity search without the SAI INDEX. */
CREATE CUSTOM INDEX IF NOT EXISTS idx_vector
ON pet_supply_vectors(product_vector)
USING 'StorageAttachedIndex';

INSERT INTO pet_supply_vectors (product_id, product_name, product_vector)
VALUES ('pf1843','HealthyFresh - Chicken raw dog food',[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0]);

INSERT INTO pet_supply_vectors (product_id, product_name, product_vector)
VALUES ('pf1844','HealthyFresh - Beef raw dog food',[1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0]);

INSERT INTO pet_supply_vectors (product_id, product_name, product_vector)
VALUES ('pt0021','Dog Tennis Ball Toy',[0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0]);

INSERT INTO pet_supply_vectors (product_id, product_name, product_vector)
VALUES ('pt0041','Dog Ring Chew Toy',[0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0]);

INSERT INTO pet_supply_vectors (product_id, product_name, product_vector)
VALUES ('pf7043','PupperSausage Bacon dog Treats',[0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1]);

INSERT INTO pet_supply_vectors (product_id, product_name, product_vector)
VALUES ('pf7044','PupperSausage Beef dog Treats',[0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0]);
