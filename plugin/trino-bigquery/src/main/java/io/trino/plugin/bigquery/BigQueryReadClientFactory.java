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

import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

import static com.google.cloud.bigquery.storage.v1.BigQueryReadSettings.defaultGrpcTransportProviderBuilder;
import static java.util.Objects.requireNonNull;

/**
 * Since Guice recommends to avoid injecting closeable resources (see
 * https://github.com/google/guice/wiki/Avoid-Injecting-Closable-Resources), this factory creates
 * short lived clients that can be closed independently.
 */
public class BigQueryReadClientFactory
{
    private final Set<BigQueryOptionsConfigurer> configurers;

    @Inject
    public BigQueryReadClientFactory(Set<BigQueryOptionsConfigurer> configurers)
    {
        this.configurers = requireNonNull(configurers, "configurers is null");
    }

    BigQueryReadClient create(ConnectorSession session)
    {
        BigQueryReadSettings.Builder builder = BigQueryReadSettings
                .newBuilder()
                .setTransportChannelProvider(defaultGrpcTransportProviderBuilder().build());

        for (BigQueryOptionsConfigurer configurer : configurers) {
            builder = configurer.configure(builder, session);
        }
        try {
            return BigQueryReadClient.create(builder.build());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error creating BigQueryReadClient", e);
        }
    }
}
