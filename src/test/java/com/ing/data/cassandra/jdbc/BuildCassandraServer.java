/*
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
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
