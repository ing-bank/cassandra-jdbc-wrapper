package com.ing.data.cassandra.jdbc.utils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;

public class FakeRetryPolicy implements RetryPolicy {

    public FakeRetryPolicy(@NonNull final DriverContext context, @NonNull final String profileName) {
        // Do nothing. For testing purpose only.
    }

    @Override
    public RetryDecision onReadTimeout(@NonNull final Request request, @NonNull final ConsistencyLevel cl,
                                       final int blockFor, final int received, final boolean dataPresent,
                                       final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onWriteTimeout(@NonNull final Request request, @NonNull final ConsistencyLevel cl,
                                        @NonNull final WriteType writeType, final int blockFor, final int received,
                                        final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onUnavailable(@NonNull final Request request, @NonNull final ConsistencyLevel cl,
                                       final int required, final int alive, final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onRequestAborted(@NonNull final Request request, @NonNull final Throwable error,
                                          final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onErrorResponse(@NonNull final Request request, @NonNull final CoordinatorException error,
                                         final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public void close() {
        // Do nothing. For testing purpose only.
    }
}
