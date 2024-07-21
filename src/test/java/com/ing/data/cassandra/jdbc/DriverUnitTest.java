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

import org.apache.commons.lang3.ArrayUtils;
import org.approvaltests.Approvals;
import org.approvaltests.strings.Printable;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.util.Properties;

import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_CONNECT_TIMEOUT;
import static com.ing.data.cassandra.jdbc.utils.JdbcUrlUtil.TAG_DEBUG;

class DriverUnitTest extends UsingCassandraContainerTest {

    private static final String KEYSPACE = "test_keyspace";

    @Test
    void givenJdbcUrlAndProperties_whenGetPropertyInfo_returnExpectedDriverPropertyInfoArray() throws Exception {
        final InetSocketAddress contactPoint = cassandraContainer.getContactPoint();
        final String jdbcUrl = buildJdbcUrl(contactPoint.getHostName(), contactPoint.getPort(), KEYSPACE,
            "localdatacenter=datacenter1");

        final Driver sut =  DriverManager.getDriver(jdbcUrl);
        final Properties props = new Properties();
        props.put(TAG_DEBUG, true);
        props.put(TAG_CONNECT_TIMEOUT, 10000);
        final DriverPropertyInfo[] result = sut.getPropertyInfo(jdbcUrl, props);
        final Printable<DriverPropertyInfo>[] printableResult = Printable.create(info ->
            info.name + ": " + info.value
                + "\n\t" + info.description
                + "\n\tchoices: " + ArrayUtils.toString(info.choices, "n/a")
                + "\n\trequired: " + info.required
        , result);
        Approvals.verifyAll("", printableResult);
    }

}
