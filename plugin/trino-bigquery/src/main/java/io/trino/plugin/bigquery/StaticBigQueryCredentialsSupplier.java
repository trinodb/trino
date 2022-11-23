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

import com.google.api.client.util.Base64;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

public class StaticBigQueryCredentialsSupplier
        implements BigQueryCredentialsSupplier
{
    private final Supplier<Optional<Credentials>> credentialsCreator;

    @Inject
    public StaticBigQueryCredentialsSupplier(StaticCredentialsConfig config)
    {
        // lazy creation, cache once it's created
        Optional<Credentials> credentialsKey = config.getCredentialsKey()
                .map(StaticBigQueryCredentialsSupplier::createCredentialsFromKey);

        Optional<Credentials> credentialsFile = config.getCredentialsFile()
                .map(StaticBigQueryCredentialsSupplier::createCredentialsFromFile);

        this.credentialsCreator = Suppliers.memoize(() -> credentialsKey.or(() -> credentialsFile));
    }

    @Override
    public Optional<Credentials> getCredentials(ConnectorSession session)
    {
        return credentialsCreator.get();
    }

    private static Credentials createCredentialsFromKey(String key)
    {
        try {
            return GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.decodeBase64(key)));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from key", e);
        }
    }

    private static Credentials createCredentialsFromFile(String file)
    {
        try {
            return GoogleCredentials.fromStream(new FileInputStream(file));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create Credentials from file", e);
        }
    }
}
