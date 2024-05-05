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

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.instaclustr.cassandra.driver.auth.KerberosAuthProviderBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Test connection using Kerberos Auth provider.
 */
class KerberosAuthProviderTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "system";

    @BeforeAll
    static void finalizeSetUpTests() throws Exception {
        initConnection(KEYSPACE, "localdatacenter=datacenter1", "usekrb5=true");
    }

    @Test
    void givenValidConnectionString_whenGetConnection_createConnectionWithKrb5Provider() throws Exception {
        assertNotNull(sqlConnection);
        assertNotNull(sqlConnection.getSession());
        assertNotNull(sqlConnection.getSession().getContext());
        final Optional<AuthProvider> configuredAuthProvider = sqlConnection.getSession().getContext().getAuthProvider();
        assertTrue(configuredAuthProvider.isPresent());
        assertTrue(KerberosAuthProviderBase.class.isAssignableFrom(configuredAuthProvider.get().getClass()));
        assertTrue(sqlConnection.getSession().getKeyspace().isPresent());
        assertEquals(KEYSPACE, sqlConnection.getSession().getKeyspace().get().asCql(true));
        sqlConnection.close();
    }

}
