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
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Optional;

public class StaticBigQueryCredentialsSupplier
        implements BigQueryCredentialsSupplier
{
    private final Supplier<Optional<Credentials>> credentialsCreator;

    @Inject
    public StaticBigQueryCredentialsSupplier(StaticCredentialsConfig config, Optional<ProxyTransportFactory> proxyTransportFactory)
    {
        Optional<HttpTransportFactory> httpTransportFactory = proxyTransportFactory
                .map(ProxyTransportFactory::getTransportOptions)
                .map(HttpTransportOptions::getHttpTransportFactory);
        // lazy creation, cache once it's created
        Optional<Credentials> credentialsKey = config.getCredentialsKey()
                .map(key -> createCredentialsFromKey(httpTransportFactory, key));

        Optional<Credentials> credentialsFile = config.getCredentialsFile()
                .map(keyFile -> createCredentialsFromFile(httpTransportFactory, keyFile));

        this.credentialsCreator = Suppliers.memoize(() -> credentialsKey.or(() -> credentialsFile));
    }

    @Override
    public Optional<Credentials> getCredentials(ConnectorSession session)
    {
        return credentialsCreator.get();
    }

    private static Credentials createCredentialsFromKey(Optional<HttpTransportFactory> httpTransportFactory, String key)
    {
        return createCredentialsFromStream(httpTransportFactory, new ByteArrayInputStream(Base64.getDecoder().decode(key)));
    }

    private static Credentials createCredentialsFromFile(Optional<HttpTransportFactory> httpTransportFactory, String file)
    {
        try {
            return createCredentialsFromStream(httpTransportFactory, new FileInputStream(file));
        }
        catch (FileNotFoundException e) {
            throw new UncheckedIOException("Failed to create Credentials from file", e);
        }
    }

    private static Credentials createCredentialsFromStream(Optional<HttpTransportFactory> httpTransportFactory, InputStream inputStream)
    {
        try {
            if (httpTransportFactory.isPresent()) {
                return GoogleCredentials.fromStream(inputStream, httpTransportFactory.get());
            }
            return GoogleCredentials.fromStream(inputStream);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from stream", e);
        }
    }
}
