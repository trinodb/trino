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
package io.trino.plugin.hive.metastore.glue;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.glue.GlueClientUtil.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class HiveGlueClientProvider
        implements Provider<AWSGlueAsync>
{
    private final GlueMetastoreStats stats;
    private final AWSCredentialsProvider credentialsProvider;
    private final GlueHiveMetastoreConfig glueConfig; // TODO do not keep mutable config instance on a field
    private final Optional<RequestHandler2> requestHandler;

    @Inject
    public HiveGlueClientProvider(
            @ForGlueHiveMetastore GlueMetastoreStats stats,
            AWSCredentialsProvider credentialsProvider,
            @ForGlueHiveMetastore Optional<RequestHandler2> requestHandler,
            GlueHiveMetastoreConfig glueConfig)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
        this.requestHandler = requireNonNull(requestHandler, "requestHandler is null");
        this.glueConfig = glueConfig;
    }

    @Override
    public AWSGlueAsync get()
    {
        return createAsyncGlueClient(glueConfig, credentialsProvider, requestHandler, stats.newRequestMetricsCollector());
    }
}
