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
package io.trino.parquet.crypto.keytools;

import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.crypto.KeyAccessDeniedException;

public interface TrinoKmsClient
{
    String KEY_ACCESS_TOKEN_DEFAULT = "DEFAULT";

    void initialize(ParquetReaderOptions trinoParquetCryptoConfig, String kmsInstanceID, String kmsInstanceURL, String accessToken)
            throws KeyAccessDeniedException;

    String wrapKey(byte[] keyBytes, String masterKeyIdentifier)
            throws KeyAccessDeniedException;

    byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier)
            throws KeyAccessDeniedException;
}
