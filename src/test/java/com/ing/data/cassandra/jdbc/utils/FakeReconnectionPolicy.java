package com.ing.data.cassandra.jdbc.utils;

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;

import static org.mockito.Mockito.mock;

public class FakeReconnectionPolicy implements ReconnectionPolicy {

    public FakeReconnectionPolicy(final DriverContext context) {
        // Do nothing. For testing purpose only.
    }

    @NonNull
    @Override
    public ReconnectionSchedule newNodeSchedule(@NonNull final Node node) {
        // Do nothing. For testing purpose only.
        return mock(ReconnectionSchedule.class);
    }

    @NonNull
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
