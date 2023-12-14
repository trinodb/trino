/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.crypto.keytools;

import org.apache.parquet.crypto.KeyAccessDeniedException;
import io.trino.parquet.ParquetReaderOptions;

public interface TrinoKmsClient
{

    /**
     * Pass configuration with KMS-specific parameters.
     *
     * @param trinoParquetCryptoConfig ...
     * @param kmsInstanceID ID of the KMS instance handled by this KmsClient. Use the default value, for KMS systems
     * that don't work with multiple instances.
     * @param kmsInstanceURL URL of the KMS instance handled by this KmsClient. Use the default value, for KMS systems
     * that don't work with URLs.
     * @param accessToken KMS access (authorization) token. Use the default value, for KMS systems that don't work with tokens.
     * @throws KeyAccessDeniedException unauthorized to initialize the KMS client
     */
    public void initialize(ParquetReaderOptions trinoParquetCryptoConfig, String kmsInstanceID, String kmsInstanceURL, String accessToken)
            throws KeyAccessDeniedException;

    /**
     * Wraps a key - encrypts it with the master key, encodes the result
     * and potentially adds a KMS-specific metadata.
     * <p>
     * If your KMS client code throws runtime exceptions related to access/permission problems
     * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
     *
     * @param keyBytes: key bytes to be wrapped
     * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
     * @return wrapped key
     * @throws KeyAccessDeniedException unauthorized to encrypt with the given master key
     */
    public String wrapKey(byte[] keyBytes, String masterKeyIdentifier)
            throws KeyAccessDeniedException;

    /**
     * Decrypts (unwraps) a key with the master key.
     * <p>
     * If your KMS client code throws runtime exceptions related to access/permission problems
     * (such as Hadoop AccessControlException), catch them and throw the KeyAccessDeniedException.
     *
     * @param wrappedKey String produced by wrapKey operation
     * @param masterKeyIdentifier: a string that uniquely identifies the master key in a KMS instance
     * @return unwrapped key bytes
     * @throws KeyAccessDeniedException unauthorized to unwrap with the given master key
     */
    public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier)
            throws KeyAccessDeniedException;
}
