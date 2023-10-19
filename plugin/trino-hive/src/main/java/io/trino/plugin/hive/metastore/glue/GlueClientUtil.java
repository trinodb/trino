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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.hdfs.s3.AwsCurrentRegionHolder.getCurrentRegionFromEC2Metadata;

public final class GlueClientUtil
{
    private GlueClientUtil() {}

    public static AWSGlueAsync createAsyncGlueClient(
            GlueHiveMetastoreConfig config,
            AWSCredentialsProvider credentialsProvider,
            Set<RequestHandler2> requestHandlers,
            RequestMetricCollector metricsCollector)
    {
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxConnections(config.getMaxGlueConnections())
                .withMaxErrorRetry(config.getMaxGlueErrorRetries());
        AWSGlueAsyncClientBuilder asyncGlueClientBuilder = AWSGlueAsyncClientBuilder.standard()
                .withMetricsCollector(metricsCollector)
                .withClientConfiguration(clientConfig);

        asyncGlueClientBuilder.setRequestHandlers(requestHandlers.toArray(RequestHandler2[]::new));

        if (config.getGlueEndpointUrl().isPresent()) {
            checkArgument(config.getGlueRegion().isPresent(), "Glue region must be set when Glue endpoint URL is set");
            asyncGlueClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                    config.getGlueEndpointUrl().get(),
                    config.getGlueRegion().get()));
        }
        else if (config.getGlueRegion().isPresent()) {
            asyncGlueClientBuilder.setRegion(config.getGlueRegion().get());
        }
        else if (config.getPinGlueClientToCurrentRegion()) {
            asyncGlueClientBuilder.setRegion(getCurrentRegionFromEC2Metadata().getName());
        }

        asyncGlueClientBuilder.setCredentials(credentialsProvider);

        return asyncGlueClientBuilder.build();
    }
}
