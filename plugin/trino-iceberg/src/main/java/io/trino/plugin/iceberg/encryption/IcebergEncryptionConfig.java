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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class IcebergEncryptionConfig
{
    private Optional<KmsType> kmsType = Optional.empty();
    private boolean plaintextFilesAllowedForEncryptedTables;

    @NotNull
    public Optional<KmsType> getKmsType()
    {
        return kmsType;
    }

    @Config("iceberg.encryption.kms-type")
    @ConfigDescription("Key Management Service type for Iceberg table encryption")
    public IcebergEncryptionConfig setKmsType(KmsType kmsType)
    {
        this.kmsType = Optional.ofNullable(kmsType);
        return this;
    }

    public boolean isPlaintextFilesAllowedForEncryptedTables()
    {
        return plaintextFilesAllowedForEncryptedTables;
    }

    @Config("iceberg.encryption.plaintext-files-allowed-for-encrypted-tables")
    @ConfigDescription("Allow reading unencrypted files in tables with encryption enabled")
    public IcebergEncryptionConfig setPlaintextFilesAllowedForEncryptedTables(boolean plaintextFilesAllowedForEncryptedTables)
    {
        this.plaintextFilesAllowedForEncryptedTables = plaintextFilesAllowedForEncryptedTables;
        return this;
    }
}
