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

    private volatile KeyManagementClient catalogKmsClient;

    @Inject
    public IcebergEncryptionManagerFactory(IcebergConfig config)
    {
        requireNonNull(config, "config is null");
        this.kmsImpl = config.getEncryptionKmsImpl();
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

        KeyManagementClient client = getOrCreateKmsClient(tableProperties);
        return EncryptionUtil.createEncryptionManager(tableProperties, client);
    }

    private KeyManagementClient getOrCreateKmsClient(Map<String, String> tableProperties)
    {
        Map<String, String> properties = new HashMap<>(tableProperties);
        String configuredKmsImpl = kmsImpl.orElse(properties.get(CatalogProperties.ENCRYPTION_KMS_IMPL));
        if (configuredKmsImpl == null || configuredKmsImpl.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg table encryption requires iceberg.encryption.kms-impl or encryption.kms-impl table property");
        }
        properties.put(CatalogProperties.ENCRYPTION_KMS_IMPL, configuredKmsImpl);

        if (kmsImpl.isEmpty()) {
            // Table-level KMS configuration can differ across tables.
            return EncryptionUtil.createKmsClient(properties);
        }

        if (catalogKmsClient != null) {
            return catalogKmsClient;
        }

        synchronized (this) {
            if (catalogKmsClient != null) {
                return catalogKmsClient;
            }
            catalogKmsClient = EncryptionUtil.createKmsClient(properties);
            return catalogKmsClient;
        }
    }
}
