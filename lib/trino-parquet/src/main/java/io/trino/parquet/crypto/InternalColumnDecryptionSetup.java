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
package io.trino.parquet.crypto;

import org.apache.parquet.format.BlockCipher;
import org.apache.parquet.hadoop.metadata.ColumnPath;

public class InternalColumnDecryptionSetup
{
    private final ColumnPath columnPath;
    private final boolean isEncrypted;
    private final boolean isEncryptedWithFooterKey;
    private final BlockCipher.Decryptor dataDecryptor;
    private final BlockCipher.Decryptor metaDataDecryptor;
    private final int columnOrdinal;
    private final byte[] keyMetadata;

    InternalColumnDecryptionSetup(
            ColumnPath path,
            boolean encrypted,
            boolean isEncryptedWithFooterKey,
            BlockCipher.Decryptor dataDecryptor,
            BlockCipher.Decryptor metaDataDecryptor,
            int columnOrdinal,
            byte[] keyMetadata)
    {
        this.columnPath = path;
        this.isEncrypted = encrypted;
        this.isEncryptedWithFooterKey = isEncryptedWithFooterKey;
        this.dataDecryptor = dataDecryptor;
        this.metaDataDecryptor = metaDataDecryptor;
        this.columnOrdinal = columnOrdinal;
        this.keyMetadata = keyMetadata;
    }

    public boolean isEncrypted()
    {
        return isEncrypted;
    }

    public BlockCipher.Decryptor getDataDecryptor()
    {
        return dataDecryptor;
    }

    public BlockCipher.Decryptor getMetaDataDecryptor()
    {
        return metaDataDecryptor;
    }

    boolean isEncryptedWithFooterKey()
    {
        return isEncryptedWithFooterKey;
    }

    ColumnPath getPath()
    {
        return columnPath;
    }

    public int getOrdinal()
    {
        return columnOrdinal;
    }

    byte[] getKeyMetadata()
    {
        return keyMetadata;
    }
}
