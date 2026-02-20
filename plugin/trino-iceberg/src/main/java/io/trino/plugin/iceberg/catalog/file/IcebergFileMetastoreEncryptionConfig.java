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
package io.trino.plugin.iceberg.catalog.file;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class IcebergFileMetastoreEncryptionConfig
{
    private Optional<String> kmsType = Optional.empty();
    private Optional<String> kmsImpl = Optional.empty();

    @NotNull
    public Optional<String> getKmsType()
    {
        return kmsType;
    }

    @Config("iceberg.file-metastore.encryption.kms-type")
    public IcebergFileMetastoreEncryptionConfig setKmsType(String kmsType)
    {
        this.kmsType = Optional.ofNullable(kmsType);
        return this;
    }

    @NotNull
    public Optional<String> getKmsImpl()
    {
        return kmsImpl;
    }

    @Config("iceberg.file-metastore.encryption.kms-impl")
    public IcebergFileMetastoreEncryptionConfig setKmsImpl(String kmsImpl)
    {
        this.kmsImpl = Optional.ofNullable(kmsImpl);
        return this;
    }
}
