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
package com.ing.data.cassandra.jdbc.testing;

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;

import jakarta.annotation.Nonnull;

import static org.mockito.Mockito.mock;

public class FakeReconnectionPolicy implements ReconnectionPolicy {

    public FakeReconnectionPolicy(final DriverContext context) {
        // Do nothing. For testing purpose only.
    }

    @Nonnull
    @Override
    public ReconnectionSchedule newNodeSchedule(@Nonnull final Node node) {
        // Do nothing. For testing purpose only.
        return mock(ReconnectionSchedule.class);
    }

    @Nonnull
    @Override
    public ReconnectionSchedule newControlConnectionSchedule(final boolean isInitialConnection) {
        // Do nothing. For testing purpose only.
        return mock(ReconnectionSchedule.class);
    }

    @Override
    public void close() {
        // Do nothing. For testing purpose only.
    }
}
