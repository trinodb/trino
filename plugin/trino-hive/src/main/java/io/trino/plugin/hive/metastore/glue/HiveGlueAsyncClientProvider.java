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

import com.google.inject.Inject;
import com.google.inject.Provider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.GlueAsyncClient;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.glue.GlueClientUtil.createAsyncGlueClient;
import static java.util.Objects.requireNonNull;

public class HiveGlueAsyncClientProvider
        implements Provider<GlueAsyncClient>
{
    private final GlueMetastoreStats stats;
    private final AwsCredentialsProvider credentialsProvider;
    private final GlueHiveMetastoreConfig glueConfig; // TODO do not keep mutable config instance on a field
    private final Optional<ExecutionInterceptor> requestHandler;

    @Inject
    public HiveGlueAsyncClientProvider(
            @ForGlueHiveMetastore GlueMetastoreStats stats,
            AwsCredentialsProvider credentialsProvider,
            @ForGlueHiveMetastore Optional<ExecutionInterceptor> requestHandler,
            GlueHiveMetastoreConfig glueConfig)
    {
        this.stats = requireNonNull(stats, "stats is null");
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
        this.requestHandler = requireNonNull(requestHandler, "requestHandler is null");
        this.glueConfig = glueConfig;
    }

    @Override
    public GlueAsyncClient get()
    {
        return createAsyncGlueClient(glueConfig, credentialsProvider, requestHandler, stats.newRequestMetricsPublisher());
    }
}
