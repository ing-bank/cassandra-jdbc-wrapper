package com.ing.data.cassandra.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.DriverManager;

abstract class UsingEmbeddedCassandraServerTest {

    static CassandraConnection sqlConnection = null;

    @BeforeAll
    static void setUpTests() {
        if (!BuildCassandraServer.isServerUp()) {
            BuildCassandraServer.initServer();
        }
    }

    @AfterAll
    static void afterTests() throws Exception {
        if (sqlConnection != null) {
            sqlConnection.close();
        }
        BuildCassandraServer.stopServer();
    }

    static void initConnection(final String keyspace, final String... parameters) throws Exception {
        sqlConnection = initConnection(BuildCassandraServer.HOST, BuildCassandraServer.PORT, keyspace, parameters);
    }

    static CassandraConnection initConnection(final String host, final int port, final String keyspace,
                                              final String... parameters) throws Exception {
        return (CassandraConnection) DriverManager.getConnection(buildJdbcUrl(host, port, keyspace, parameters));
    }

    static String buildJdbcUrl(final String host, final int port, final String keyspace, final String... parameters) {
        String joinedParameters = String.join("&", parameters);
        if (StringUtils.isNotBlank(joinedParameters)) {
            joinedParameters = StringUtils.prependIfMissing(joinedParameters, "?");
        }

        return String.format("jdbc:cassandra://%s:%d/%s%s", host, port, keyspace, joinedParameters);
    }
}
