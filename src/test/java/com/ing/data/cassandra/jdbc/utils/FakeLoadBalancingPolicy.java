package com.ing.data.cassandra.jdbc.utils;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;

import static org.mockito.Mockito.mock;

public class FakeLoadBalancingPolicy implements LoadBalancingPolicy {

    public FakeLoadBalancingPolicy(@NonNull final DriverContext context, @NonNull final String profileName) {
        // Do nothing. For testing purpose only.
    }

    @Override
    public void init(@NonNull final Map<UUID, Node> nodes, @NonNull final DistanceReporter distanceReporter) {
        // Do nothing. For testing purpose only.
    }

    @NonNull
    @Override
    public Queue<Node> newQueryPlan(@Nullable final Request request, @Nullable final Session session) {
        // Do nothing. For testing purpose only.
        return new QueryPlan(mock(Node.class));
    }

    @Override
    public void onAdd(@NonNull final Node node) {
        // Do nothing. For testing purpose only.
    }

    @Override
    public void onUp(@NonNull final Node node) {
        // Do nothing. For testing purpose only.
    }

    @Override
    public void onDown(@NonNull final Node node) {
        // Do nothing. For testing purpose only.
    }

    @Override
    public void onRemove(@NonNull final Node node) {
        // Do nothing. For testing purpose only.
    }

    @Override
    public void close() {
        // Do nothing. For testing purpose only.
    }
}
