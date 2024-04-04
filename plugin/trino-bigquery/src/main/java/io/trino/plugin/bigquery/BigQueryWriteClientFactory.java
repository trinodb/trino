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

import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class BigQueryWriteClientFactory
{
    private final Set<BigQueryOptionsConfigurer> configurers;

    @Inject
    public BigQueryWriteClientFactory(Set<BigQueryOptionsConfigurer> configurers)
    {
        this.configurers = ImmutableSet.copyOf(requireNonNull(configurers, "configurers is null"));
    }

    public BigQueryWriteClient create(ConnectorSession session)
    {
        BigQueryWriteSettings.Builder builder = BigQueryWriteSettings.newBuilder();

        for (BigQueryOptionsConfigurer configurer : configurers) {
            builder = configurer.configure(builder, session);
        }
        try {
            return BigQueryWriteClient.create(builder.build());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error creating BigQueryWriteClient", e);
        }
    }
}
