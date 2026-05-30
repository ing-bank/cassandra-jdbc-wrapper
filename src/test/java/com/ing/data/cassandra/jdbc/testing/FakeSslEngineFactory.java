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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;

import jakarta.annotation.Nonnull;
import javax.net.ssl.SSLEngine;

public class FakeSslEngineFactory implements SslEngineFactory {

    public FakeSslEngineFactory() {
        // Do nothing. For testing purpose only.
    }

    @Nonnull
    @Override
    public SSLEngine newSslEngine(@Nonnull EndPoint remoteEndpoint) {
        // Do nothing. For testing purpose only.
        return null;
    }

    @Override
    public void close() throws Exception {
        // Do nothing. For testing purpose only.
    }

}
