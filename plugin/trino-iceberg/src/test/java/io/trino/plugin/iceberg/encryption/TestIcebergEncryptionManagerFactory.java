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
import io.trino.plugin.iceberg.IcebergConfig;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergEncryptionManagerFactory
{
    @Test
    public void testReturnsPlaintextManagerWhenEncryptionDisabled()
    {
        IcebergEncryptionManagerFactory factory = new IcebergEncryptionManagerFactory(new IcebergConfig());
        assertThat(factory.createEncryptionManager(ImmutableMap.of()))
                .isSameAs(PlaintextEncryptionManager.instance());
    }

    @Test
    public void testDoesNotReuseTableLevelKmsClientAcrossTables()
    {
        IcebergEncryptionManagerFactory factory = new IcebergEncryptionManagerFactory(new IcebergConfig());
        Map<String, String> validKmsProperties = ImmutableMap.of(
                TableProperties.ENCRYPTION_TABLE_KEY, "key-a",
                CatalogProperties.ENCRYPTION_KMS_IMPL, TestingKmsClient.class.getName());
        Map<String, String> invalidKmsProperties = ImmutableMap.of(
                TableProperties.ENCRYPTION_TABLE_KEY, "key-b",
                CatalogProperties.ENCRYPTION_KMS_IMPL, "invalid.kms.Client");

        assertThat(factory.createEncryptionManager(validKmsProperties)).isNotNull();
        assertThatThrownBy(() -> factory.createEncryptionManager(invalidKmsProperties))
                .isInstanceOf(RuntimeException.class);
    }
}
