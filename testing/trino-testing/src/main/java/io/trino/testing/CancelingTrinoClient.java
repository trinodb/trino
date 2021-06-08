/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.testing;

import io.trino.Session;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.server.testing.TestingTrinoServer;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class CancelingTrinoClient
        extends AbstractTestingTrinoClient<Void>
{
    private final OkHttpClient httpClient;

    protected CancelingTrinoClient(TestingTrinoServer trinoServer, Session defaultSession)
    {
        this(trinoServer, defaultSession, new OkHttpClient());
    }

    public CancelingTrinoClient(TestingTrinoServer trinoServer, Session defaultSession, OkHttpClient httpClient)
    {
        super(trinoServer, defaultSession, httpClient);
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @Override
    protected ResultsSession<Void> getResultSession(Session session)
    {
        return new CancelingResultSession(httpClient);
    }

    private static class CancelingResultSession
            implements ResultsSession<Void>
    {
        private final OkHttpClient httpClient;

        public CancelingResultSession(OkHttpClient httpClient)
        {
            this.httpClient = requireNonNull(httpClient, "httpClient is null");
        }

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            // Make sure the query is fully planned.
            if (statusInfo.getStats().getRunningSplits() <= 0) {
                return;
            }

            if (statusInfo.getPartialCancelUri() == null) {
                // nothing to cancel
                return;
            }

            try {
                Request request = new Request.Builder()
                        .url(statusInfo.getPartialCancelUri().toURL())
                        .delete()
                        .build();
                httpClient.newCall(request)
                        .execute()
                        .close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }
    }
}
