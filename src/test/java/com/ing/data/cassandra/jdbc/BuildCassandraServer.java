package com.ing.data.cassandra.jdbc;

import info.archinnov.achilles.embedded.CassandraEmbeddedServer;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

final class BuildCassandraServer {

    static final String HOST = System.getProperty("host", ConnectionDetails.getHost());
    static final int PORT = Integer.parseInt(System.getProperty("port", String.valueOf(ConnectionDetails.getPort())));

    static CassandraEmbeddedServer server = null;
    private static boolean serverIsUp = false;

    static void initServer() {
        server = CassandraEmbeddedServerBuilder.builder()
            .withClusterName("embedded_test_cluster")
            .withListenAddress(HOST)
            .withCQLPort(PORT)
            .cleanDataFilesAtStartup(true)
            .withScript("initEmbeddedCassandra.cql")
            .buildServer();

        serverIsUp = true;
    }

    static void stopServer() {
        if (server != null && serverIsUp) {
            server.getNativeCluster().close();
        }
    }

    static boolean isServerUp() {
        return serverIsUp;
    }
}
