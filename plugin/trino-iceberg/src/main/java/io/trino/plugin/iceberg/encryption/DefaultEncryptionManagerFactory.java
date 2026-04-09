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
import io.trino.spi.TrinoException;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.ENCRYPTION_KMS_IMPL;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

public class DefaultEncryptionManagerFactory
        implements EncryptionManagerFactory
{
    private final Optional<KeyManagementClient> keyManagementClient;

    @Inject
    public DefaultEncryptionManagerFactory(IcebergEncryptionConfig encryptionConfig)
    {
        this.keyManagementClient = createKeyManagementClient(requireNonNull(encryptionConfig, "encryptionConfig is null"));
    }

    public DefaultEncryptionManagerFactory(Optional<KeyManagementClient> keyManagementClient)
    {
        this.keyManagementClient = requireNonNull(keyManagementClient, "keyManagementClient is null");
    }

    @Override
    public EncryptionManager create(Map<String, String> tableProperties)
    {
        String tableKeyId = tableProperties.get(ENCRYPTION_TABLE_KEY);
        if (tableKeyId == null) {
            return PlaintextEncryptionManager.instance();
        }

        KeyManagementClient kmsClient = keyManagementClient.orElseThrow(() -> new TrinoException(
                ICEBERG_CATALOG_ERROR,
                "Can't create encryption manager, because key management client is not configured. Set iceberg.encryption.kms-type catalog property."));
        return EncryptionUtil.createEncryptionManager(tableProperties, kmsClient);
    }

    private static Optional<KeyManagementClient> createKeyManagementClient(IcebergEncryptionConfig encryptionConfig)
    {
        if (encryptionConfig.getKmsType().isEmpty()) {
            return Optional.empty();
        }

        Map<String, String> catalogProperties = ImmutableMap.of(
                ENCRYPTION_KMS_IMPL, encryptionConfig.getKmsType().orElseThrow().getKmsClientClassName());
        return Optional.of(EncryptionUtil.createKmsClient(catalogProperties));
    }
}
