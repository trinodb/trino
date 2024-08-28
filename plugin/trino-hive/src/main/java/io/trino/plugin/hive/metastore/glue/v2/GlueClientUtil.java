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
package io.trino.plugin.hive.metastore.glue.v2;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;

import java.net.URI;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.metastore.glue.v2.AwsCurrentRegionHolder.getCurrentRegionFromEc2Metadata;
import static java.time.Duration.ofMillis;
import static software.amazon.awssdk.retries.api.BackoffStrategy.exponentialDelayHalfJitter;

public final class GlueClientUtil
{
    private GlueClientUtil() {}

    public static GlueClient createGlueClient(
            GlueHiveMetastoreConfig config,
            AwsCredentialsProvider credentialsProvider,
            Set<ExecutionInterceptor> interceptors,
            MetricPublisher metricsCollector)
    {
        RetryStrategy retryStrategy = StandardRetryStrategy.builder()
                .retryOnException(exception -> exception instanceof ConcurrentModificationException)
                .backoffStrategy(exponentialDelayHalfJitter(ofMillis(20), ofMillis(1500)))
                .throttlingBackoffStrategy(exponentialDelayHalfJitter(ofMillis(20), ofMillis(1500)))
                .maxAttempts(config.getMaxGlueErrorRetries())
                .build();

        SdkHttpClient client = ApacheHttpClient.builder()
                .maxConnections(config.getMaxGlueConnections())
                .build();

        GlueClientBuilder glueBuilder = GlueClient.builder()
                .httpClient(client)
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(builder -> {
                    builder
                            .retryStrategy(retryStrategy)
                            .addMetricPublisher(metricsCollector);

                    interceptors.forEach(builder::addExecutionInterceptor);
                });

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            glueBuilder
                    .endpointOverride(URI.create(config.getGlueEndpointUrl().get()))
                    .region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            glueBuilder.region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            glueBuilder.region(getCurrentRegionFromEc2Metadata());
        }

        return glueBuilder.build();
    }

    public static GlueAsyncClient createAsyncGlueClient(
            GlueHiveMetastoreConfig config,
            AwsCredentialsProvider credentialsProvider,
            Set<ExecutionInterceptor> interceptors,
            MetricPublisher metricsCollector)
    {
        RetryStrategy retryStrategy = StandardRetryStrategy.builder()
                .retryOnException(exception -> exception instanceof ConcurrentModificationException)
                .backoffStrategy(exponentialDelayHalfJitter(ofMillis(20), ofMillis(1500)))
                .throttlingBackoffStrategy(exponentialDelayHalfJitter(ofMillis(20), ofMillis(1500)))
                .maxAttempts(config.getMaxGlueErrorRetries())
                .build();

        SdkAsyncHttpClient client = NettyNioAsyncHttpClient.builder()
                .maxConcurrency(config.getMaxGlueConnections())
                .build();

        GlueAsyncClientBuilder glueBuilder = GlueAsyncClient.builder()
                .httpClient(client)
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(builder -> {
                    builder
                            .retryStrategy(retryStrategy)
                            .addMetricPublisher(metricsCollector);

                    interceptors.forEach(builder::addExecutionInterceptor);
                });

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            glueBuilder
                    .endpointOverride(URI.create(config.getGlueEndpointUrl().get()))
                    .region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            glueBuilder.region(Region.of(config.getGlueRegion().get()));
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            glueBuilder.region(getCurrentRegionFromEc2Metadata());
        }

        return glueBuilder.build();
    }
}
