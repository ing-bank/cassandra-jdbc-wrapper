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
package com.ing.data.cassandra.jdbc.utils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;

import javax.annotation.Nonnull;

public class FakeRetryPolicy implements RetryPolicy {

    public FakeRetryPolicy(final DriverContext context, final String profileName) {
        // Do nothing. For testing purpose only.
    }

    @Override
    public RetryDecision onReadTimeout(@Nonnull final Request request, @Nonnull final ConsistencyLevel cl,
                                       final int blockFor, final int received, final boolean dataPresent,
                                       final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onWriteTimeout(@Nonnull final Request request, @Nonnull final ConsistencyLevel cl,
                                        @Nonnull final WriteType writeType, final int blockFor, final int received,
                                        final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onUnavailable(@Nonnull final Request request, @Nonnull final ConsistencyLevel cl,
                                       final int required, final int alive, final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onRequestAborted(@Nonnull final Request request, @Nonnull final Throwable error,
                                          final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public RetryDecision onErrorResponse(@Nonnull final Request request, @Nonnull final CoordinatorException error,
                                         final int retryCount) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public void close() {
        // Do nothing. For testing purpose only.
    }
}
