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

import java.util.Optional;

/**
 * Interface for classes retrieving encryption keys using the key metadata.
 * Implementations must be thread-safe, if same {@link DecryptionKeyRetriever} object is passed to multiple file readers.
 */
public interface DecryptionKeyRetriever
{
    /**
     * Returns key for a given column and the key metadata. Should return empty if user does not have access to the column key or key doesn't exist.
     */
    Optional<byte[]> getColumnKey(ColumnPath columnPath, Optional<byte[]> keyMetadata);

    /**
     * Returns key for a footer and the key metadata. Should return empty if user does not have access to the column key or key doesn't exist.
     */
    Optional<byte[]> getFooterKey(Optional<byte[]> keyMetadata);
}
