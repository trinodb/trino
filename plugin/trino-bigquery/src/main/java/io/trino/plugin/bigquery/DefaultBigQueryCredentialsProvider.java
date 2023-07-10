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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DefaultBigQueryCredentialsProvider
        implements BigQueryCredentialsSupplier
{
    private final Optional<ProxyTransportFactory> transportFactory;

    @Inject
    public DefaultBigQueryCredentialsProvider(Optional<ProxyTransportFactory> transportFactory)
    {
        this.transportFactory = requireNonNull(transportFactory, "transportFactory is null");
    }

    @Override
    public Optional<Credentials> getCredentials(ConnectorSession session)
    {
        return transportFactory.map(factory -> {
            try {
                return GoogleCredentials.getApplicationDefault(factory.getTransportOptions().getHttpTransportFactory());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
