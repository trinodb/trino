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
package io.trino.plugin.bigquery;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.connector.ConnectorSession;

import static java.lang.Math.pow;
import static java.util.Objects.requireNonNull;
import static org.threeten.bp.temporal.ChronoUnit.MILLIS;

public class RetryOptionsConfigurer
        implements BigQueryOptionsConfigurer
{
    private final int retries;
    private final Duration timeout;
    private final Duration retryDelay;
    private final double retryMultiplier;

    @Inject
    public RetryOptionsConfigurer(BigQueryRpcConfig rpcConfig)
    {
        requireNonNull(rpcConfig, "rpcConfig is null");
        this.retries = rpcConfig.getRetries();
        this.timeout = rpcConfig.getTimeout();
        this.retryDelay = rpcConfig.getRetryDelay();
        this.retryMultiplier = rpcConfig.getRetryMultiplier();
    }

    @Override
    public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
    {
        return builder.setRetrySettings(retrySettings());
    }

    @Override
    public BigQueryReadSettings.Builder configure(BigQueryReadSettings.Builder builder, ConnectorSession session)
    {
        try {
            return builder.applyToAllUnaryMethods(methodBuilder -> {
                methodBuilder.setRetrySettings(retrySettings());
                return null;
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private RetrySettings retrySettings()
    {
        long maxDelay = retryDelay.toMillis() * (long) pow(retryMultiplier, retries);

        return RetrySettings.newBuilder()
                .setMaxAttempts(retries)
                .setTotalTimeout(org.threeten.bp.Duration.of(timeout.toMillis(), MILLIS))
                .setInitialRetryDelay(org.threeten.bp.Duration.of(retryDelay.toMillis(), MILLIS))
                .setRetryDelayMultiplier(retryMultiplier)
                .setMaxRetryDelay(org.threeten.bp.Duration.of(maxDelay, MILLIS))
                .build();
    }
}
