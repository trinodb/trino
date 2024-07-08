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
package io.trino.configuration.keystore;

import com.google.inject.Inject;
import io.trino.spi.configuration.ConfigurationValueResolver;
import io.trino.spi.configuration.InvalidConfigurationException;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

import static io.trino.configuration.keystore.KeyStoreUtils.loadKeyStore;
import static io.trino.configuration.keystore.KeyStoreUtils.readEntity;
import static io.trino.configuration.keystore.KeystoreErrorCode.KEYSTORE_ERROR_CODE;

public class KeystoreConfigurationResolver
        implements ConfigurationValueResolver
{
    private final KeyStore keyStore;
    private final String keystorePassword;

    @Inject
    public KeystoreConfigurationResolver(KeystoreConfigurationResolverConfig config)
            throws GeneralSecurityException, IOException
    {
        keyStore = loadKeyStore(config.getKeyStoreType(), config.getKeyStoreFilePath(), config.getKeyStorePassword());
        this.keystorePassword = config.getKeyStorePassword();
    }

    @Override
    public String resolveConfigurationValue(String key)
            throws InvalidConfigurationException
    {
        try {
            return readEntity(keyStore, key, keystorePassword);
        }
        catch (GeneralSecurityException e) {
            throw new InvalidConfigurationException(KEYSTORE_ERROR_CODE, e.getMessage());
        }
    }
}
