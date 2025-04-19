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

import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.nio.charset.StandardCharsets;

public class ColumnEncryptionProperties
{
    private final boolean encrypted;
    private final ColumnPath columnPath;
    private final boolean encryptedWithFooterKey;
    private final byte[] keyBytes;
    private final byte[] keyMetaData;

    private ColumnEncryptionProperties(boolean encrypted, ColumnPath columnPath, byte[] keyBytes, byte[] keyMetaData)
    {
        if (null == columnPath) {
            throw new IllegalArgumentException("Null column path");
        }
        if (!encrypted) {
            if (null != keyBytes) {
                throw new IllegalArgumentException("Setting key on unencrypted column: " + columnPath);
            }
            if (null != keyMetaData) {
                throw new IllegalArgumentException("Setting key metadata on unencrypted column: " + columnPath);
            }
        }
        if ((null != keyBytes) && !(keyBytes.length == 16 || keyBytes.length == 24 || keyBytes.length == 32)) {
            throw new IllegalArgumentException("Wrong key length: " + keyBytes.length + ". Column: " + columnPath);
        }
        encryptedWithFooterKey = (encrypted && (null == keyBytes));
        if (encryptedWithFooterKey && (null != keyMetaData)) {
            throw new IllegalArgumentException(
                    "Setting key metadata on column encrypted with footer key:  " + columnPath);
        }

        this.encrypted = encrypted;
        this.columnPath = columnPath;
        this.keyBytes = keyBytes;
        this.keyMetaData = keyMetaData;
    }

    /**
     * Convenience builder for regular (not nested) columns.
     * To make sure column name is not misspelled or misplaced,
     * file writer will verify that column is in file schema.
     *
     * @param name Flat column name
     * @return Builder
     */
    public static Builder builder(String name)
    {
        return builder(ColumnPath.get(name), true);
    }

    /**
     * Builder for encrypted columns.
     * To make sure column path is not misspelled or misplaced,
     * file writer will verify this column is in file schema.
     *
     * @param path Column path
     * @return Builder
     */
    public static Builder builder(ColumnPath path)
    {
        return builder(path, true);
    }

    /**
     * Builder for encrypted columns.
     * To make sure column path is not misspelled or misplaced,
     * file writer will verify this column is in file schema.
     *
     * @param path Column path
     * @param encrypt whether or not this column to be encrypted
     * @return Builder
     */
    public static Builder builder(ColumnPath path, boolean encrypt)
    {
        return new Builder(path, encrypt);
    }

    public static class Builder
    {
        private final boolean encrypted;
        private final ColumnPath columnPath;

        private byte[] keyBytes;
        private byte[] keyMetaData;

        private Builder(ColumnPath path, boolean encrypted)
        {
            this.encrypted = encrypted;
            this.columnPath = path;
        }

        /**
         * Set a column-specific key.
         * If key is not set on an encrypted column, the column will
         * be encrypted with the footer key.
         *
         * @param columnKey Key length must be either 16, 24 or 32 bytes.
         * @return Builder
         */
        public Builder withKey(byte[] columnKey)
        {
            if (null == columnKey) {
                return this;
            }
            if (null != this.keyBytes) {
                throw new IllegalStateException("Key already set on column: " + columnPath);
            }
            this.keyBytes = new byte[columnKey.length];
            System.arraycopy(columnKey, 0, this.keyBytes, 0, columnKey.length);

            return this;
        }

        /**
         * Set a key retrieval metadata.
         * use either withKeyMetaData or withKeyID, not both.
         *
         * @param keyMetaData arbitrary byte array with encryption key metadata
         * @return Builder
         */
        public Builder withKeyMetaData(byte[] keyMetaData)
        {
            if (null == keyMetaData) {
                return this;
            }
            if (null != this.keyMetaData) {
                throw new IllegalStateException("Key metadata already set on column: " + columnPath);
            }
            this.keyMetaData = keyMetaData;

            return this;
        }

        /**
         * Set a key retrieval metadata (converted from String).
         * use either withKeyMetaData or withKeyID, not both.
         *
         * @param keyId will be converted to metadata (UTF-8 array).
         * @return Builder
         */
        public Builder withKeyID(String keyId)
        {
            if (null == keyId) {
                return this;
            }
            byte[] metaData = keyId.getBytes(StandardCharsets.UTF_8);

            return withKeyMetaData(metaData);
        }

        public ColumnEncryptionProperties build()
        {
            return new ColumnEncryptionProperties(encrypted, columnPath, keyBytes, keyMetaData);
        }
    }

    public ColumnPath getPath()
    {
        return columnPath;
    }

    public boolean isEncrypted()
    {
        return encrypted;
    }

    public byte[] getKeyBytes()
    {
        return keyBytes;
    }

    public boolean isEncryptedWithFooterKey()
    {
        if (!encrypted) {
            return false;
        }
        return encryptedWithFooterKey;
    }

    public byte[] getKeyMetaData()
    {
        return keyMetaData;
    }
}
