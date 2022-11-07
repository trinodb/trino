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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Since Guice recommends to avoid injecting closeable resources (see
 * https://github.com/google/guice/wiki/Avoid-Injecting-Closable-Resources), this factory creates
 * short lived clients that can be closed independently.
 */
public class BigQueryReadClientFactory
{
    private final BigQueryCredentialsSupplier credentialsSupplier;
    private final HeaderProvider headerProvider;
    private final ChannelPoolSettings channelPoolSettings;

    @Inject
    public BigQueryReadClientFactory(BigQueryConfig bigQueryConfig, BigQueryCredentialsSupplier bigQueryCredentialsSupplier, HeaderProvider headerProvider)
    {
        requireNonNull(bigQueryConfig, "bigQueryConfig is null");
        this.credentialsSupplier = requireNonNull(bigQueryCredentialsSupplier, "credentialsSupplier is null");
        this.headerProvider = requireNonNull(headerProvider, "headerProvider is null");

        this.channelPoolSettings = ChannelPoolSettings.builder()
                .setInitialChannelCount(bigQueryConfig.getRpcInitialChannelCount())
                .setMinChannelCount(bigQueryConfig.getRpcMinChannelCount())
                .setMaxChannelCount(bigQueryConfig.getRpcMaxChannelCount())
                .setMinRpcsPerChannel(bigQueryConfig.getMinRpcPerChannel())
                .setMaxRpcsPerChannel(bigQueryConfig.getMaxRpcPerChannel())
                .build();
    }

    BigQueryReadClient create(ConnectorSession session)
    {
        Optional<Credentials> credentials = credentialsSupplier.getCredentials(session);

        try {
            BigQueryReadSettings.Builder clientSettings = BigQueryReadSettings.newBuilder()
                    .setTransportChannelProvider(
                            BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
                                    .setHeaderProvider(headerProvider)
                                    .setChannelPoolSettings(channelPoolSettings)
                                    .build());
            credentials.ifPresent(value ->
                    clientSettings.setCredentialsProvider(FixedCredentialsProvider.create(value)));
            return BigQueryReadClient.create(clientSettings.build());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error creating BigQueryReadClient", e);
        }
    }
}
