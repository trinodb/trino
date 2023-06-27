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

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public final class GlueClientUtil
{
    private GlueClientUtil() {}

    public static GlueAsyncClient createAsyncGlueClient(
            GlueHiveMetastoreConfig config,
            AwsCredentialsProvider credentialsProvider,
            Optional<ExecutionInterceptor> requestHandler,
            MetricPublisher metricPublisher)
    {
        NettyNioAsyncHttpClient.Builder nettyBuilder = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(config.getMaxGlueConnections());
        ClientOverrideConfiguration clientOverrideConfiguration =
                createClientOverrideConfiguration(config, requestHandler, metricPublisher);
        GlueAsyncClientBuilder glueAsyncClientBuilder = GlueAsyncClient.builder()
                .httpClient(nettyBuilder.build())
                .overrideConfiguration(clientOverrideConfiguration);

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            glueAsyncClientBuilder
                    .endpointOverride(URI.create(config.getGlueEndpointUrl().get()))
                    .region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            glueAsyncClientBuilder.region(Region.of(config.getGlueRegion().get()));
        }
        glueAsyncClientBuilder.credentialsProvider(credentialsProvider);

        return glueAsyncClientBuilder.build();
    }

    public static GlueClient createSyncGlueClient(
            GlueHiveMetastoreConfig config,
            AwsCredentialsProvider credentialsProvider,
            Optional<ExecutionInterceptor> requestHandler,
            MetricPublisher metricPublisher)
    {
        ApacheHttpClient.Builder apacheHttpClientbuilder = ApacheHttpClient.builder()
                .maxConnections(config.getMaxGlueConnections());

        ClientOverrideConfiguration clientOverrideConfiguration =
                createClientOverrideConfiguration(config, requestHandler, metricPublisher);

        GlueClientBuilder glueClientBuilder = GlueClient.builder()
                .httpClient(apacheHttpClientbuilder.build())
                .overrideConfiguration(clientOverrideConfiguration);

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            glueClientBuilder
                    .endpointOverride(URI.create(config.getGlueEndpointUrl().get()))
                    .region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            glueClientBuilder.region(Region.of(config.getGlueRegion().get()));
        }
        glueClientBuilder.credentialsProvider(credentialsProvider);
        return glueClientBuilder.build();
    }

    private static ClientOverrideConfiguration createClientOverrideConfiguration(
            GlueHiveMetastoreConfig config, Optional<ExecutionInterceptor> requestHandler,
            MetricPublisher metricPublisher)
    {
        RetryPolicy.Builder retryPolicy = RetryPolicy.builder().numRetries(config.getMaxGlueErrorRetries());
        ClientOverrideConfiguration.Builder clientOverrideConfiguration = ClientOverrideConfiguration.builder()
                .addMetricPublisher(metricPublisher)
                .retryPolicy(retryPolicy.build());

        ImmutableList.Builder<ExecutionInterceptor> requestHandlers = ImmutableList.builder();
        requestHandler.ifPresent(requestHandlers::add);
        config.getCatalogId().ifPresent(catalogId -> requestHandlers.add(new GlueCatalogIdRequestHandler(catalogId)));
        clientOverrideConfiguration.executionInterceptors(requestHandlers.build());
        return clientOverrideConfiguration.build();
    }
}
