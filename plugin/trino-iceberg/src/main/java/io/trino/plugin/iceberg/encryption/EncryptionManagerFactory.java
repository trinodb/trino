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

import org.apache.iceberg.encryption.EncryptionManager;

import java.util.Map;

/**
 * Factory for creating Iceberg {@link EncryptionManager} instances based on table properties.
 * Returns {@link org.apache.iceberg.encryption.PlaintextEncryptionManager} when the table
 * is not encrypted, or an encryption-capable manager when encryption is configured.
 */
public interface EncryptionManagerFactory
{
    EncryptionManager create(Map<String, String> tableProperties);
}
