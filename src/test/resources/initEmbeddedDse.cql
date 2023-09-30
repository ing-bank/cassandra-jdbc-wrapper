/* Init keyspace and tables for VectorsUnitTest */
DROP KEYSPACE IF EXISTS test_keyspace_vect;
CREATE KEYSPACE "test_keyspace_vect" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace_vect;

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
