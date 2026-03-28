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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergConfig;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergEncryptionManagerFactory
{
    @Test
    void testReturnsPlaintextManagerWhenEncryptionDisabled()
    {
        IcebergEncryptionManagerFactory factory = new IcebergEncryptionManagerFactory(new IcebergConfig());
        assertThat(factory.createEncryptionManager(ImmutableMap.of()))
                .isSameAs(PlaintextEncryptionManager.instance());
    }

    @Test
    void testRequiresCatalogKmsConfigurationWhenEncryptionEnabled()
    {
        IcebergEncryptionManagerFactory factory = new IcebergEncryptionManagerFactory(new IcebergConfig());

        assertThatThrownBy(() -> factory.createEncryptionManager(ImmutableMap.of(
                TableProperties.ENCRYPTION_TABLE_KEY, "key",
                "encryption.kms-impl", "test.kms.Client")))
                .hasMessageContaining("Iceberg table encryption requires iceberg.encryption.kms-type catalog property");
    }

    @Test
    void testCatalogKmsClientUsesKmsProperties()
    {
        IcebergEncryptionManagerFactory factory = new IcebergEncryptionManagerFactory(new IcebergConfig()
                .setEncryptionKmsType(IcebergConfig.EncryptionKmsType.AWS)
                .setEncryptionKmsProperties(ImmutableList.of(
                        "client.factory=" + TestingPropertyAwareAwsClientFactory.class.getName(),
                        TestingPropertyAwareAwsClientFactory.REQUIRED_PROPERTY + "=catalog-value")));

        assertThat(factory.createEncryptionManager(ImmutableMap.of(TableProperties.ENCRYPTION_TABLE_KEY, "key")))
                .isNotNull();
    }

    @Test
    void testRejectsInvalidKmsPropertiesEntry()
    {
        assertThatThrownBy(() -> new IcebergEncryptionManagerFactory(
                new IcebergConfig()
                        .setEncryptionKmsType(IcebergConfig.EncryptionKmsType.AWS)
                        .setEncryptionKmsProperties(ImmutableList.of("invalid"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid iceberg.encryption.kms-properties entry");
    }
}
