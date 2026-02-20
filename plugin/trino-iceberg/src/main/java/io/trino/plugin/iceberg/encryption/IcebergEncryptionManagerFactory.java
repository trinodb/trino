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
package io.trino.plugin.iceberg.encryption;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.spi.TrinoException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class IcebergEncryptionManagerFactory
{
    private final Optional<String> kmsImpl;
    private final Map<String, String> kmsProperties;

    private volatile KeyManagementClient catalogKmsClient;

    @Inject
    public IcebergEncryptionManagerFactory(IcebergConfig config)
    {
        // TODO: Once Iceberg commit https://github.com/apache/iceberg/commit/8c2ca1d084fca37671ba8b38d59ea3f5a187b147 is available in the version we use,
        // switch to CatalogProperties.ENCRYPTION_KMS_TYPE instead of mapping to encryption.kms-impl.
        this.kmsImpl = config.getEncryptionKmsType()
                .map(IcebergConfig.EncryptionKmsType::getKmsClientClassName);
        this.kmsProperties = parseKmsProperties(config.getEncryptionKmsProperties());
    }

    public EncryptionManager createEncryptionManager(TableMetadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        return createEncryptionManager(metadata.properties());
    }

    public EncryptionManager createEncryptionManager(Map<String, String> tableProperties)
    {
        requireNonNull(tableProperties, "tableProperties is null");
        if (!tableProperties.containsKey(TableProperties.ENCRYPTION_TABLE_KEY)) {
            return PlaintextEncryptionManager.instance();
        }

        KeyManagementClient client = getOrCreateKmsClient();
        return EncryptionUtil.createEncryptionManager(tableProperties, client);
    }

    private KeyManagementClient getOrCreateKmsClient()
    {
        String configuredKmsImpl = kmsImpl.orElse(null);
        if (configuredKmsImpl == null || configuredKmsImpl.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg table encryption requires iceberg.encryption.kms-type catalog property");
        }

        if (catalogKmsClient != null) {
            return catalogKmsClient;
        }

        synchronized (this) {
            if (catalogKmsClient != null) {
                return catalogKmsClient;
            }
            Map<String, String> properties = new HashMap<>(kmsProperties);
            // Iceberg 1.10.x does not support encryption.kms-type yet, so set the KMS impl explicitly.
            properties.remove(CatalogProperties.ENCRYPTION_KMS_TYPE);
            properties.put(CatalogProperties.ENCRYPTION_KMS_IMPL, configuredKmsImpl);
            catalogKmsClient = EncryptionUtil.createKmsClient(properties);
            return catalogKmsClient;
        }
    }

    private static Map<String, String> parseKmsProperties(Iterable<String> kmsProperties)
    {
        Map<String, String> properties = new HashMap<>();
        for (String property : kmsProperties) {
            int delimiter = property.indexOf('=');
            if (delimiter <= 0 || delimiter == property.length() - 1) {
                throw new IllegalArgumentException("Invalid iceberg.encryption.kms-properties entry: " + property);
            }
            String key = property.substring(0, delimiter).trim();
            String value = property.substring(delimiter + 1).trim();
            if (key.isEmpty() || value.isEmpty()) {
                throw new IllegalArgumentException("Invalid iceberg.encryption.kms-properties entry: " + property);
            }
            properties.put(key, value);
        }
        return ImmutableMap.copyOf(properties);
    }
}
