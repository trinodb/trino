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

/**
 * Interface for classes retrieving encryption keys using the key metadata.
 * Implementations must be thread-safe, if same KeyRetriever object is passed to multiple file readers.
 */
public interface DecryptionKeyRetriever
{
    /**
     * Returns encryption key using the key metadata.
     * If your key retrieval code throws runtime exceptions related to access control (permission) problems
     * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
     *
     * @param keyMetaData arbitrary byte array with encryption key metadata
     * @return encryption key. Key length can be either 16, 24 or 32 bytes.
     * @throws KeyAccessDeniedException thrown upon access control problems (authentication or authorization)
     * @throws ParquetCryptoRuntimeException thrown upon key retrieval problems unrelated to access control
     */
    byte[] getKey(byte[] keyMetaData)
            throws KeyAccessDeniedException, ParquetCryptoRuntimeException;
}
