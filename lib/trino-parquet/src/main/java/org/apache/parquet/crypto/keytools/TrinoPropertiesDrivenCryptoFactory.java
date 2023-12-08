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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import org.apache.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.parquet.crypto.TrinoDecryptionPropertiesFactory;
import org.apache.parquet.crypto.TrinoParquetCryptoConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;

public class TrinoPropertiesDrivenCryptoFactory
        implements TrinoDecryptionPropertiesFactory
{
  private static final Logger LOG = LoggerFactory.getLogger(PropertiesDrivenCryptoFactory.class);

  private static final int[] ACCEPTABLE_DATA_KEY_LENGTHS = {128, 192, 256};

  /**
   * List of columns to encrypt, with master key IDs (see HIVE-21848).
   * Format: "masterKeyID:colName,colName;masterKeyID:colName...".
   * Unlisted columns are not encrypted.
   */
  public static final String COLUMN_KEYS_PROPERTY_NAME = "parquet.encryption.column.keys";
  /**
   * Master key ID for footer encryption/signing.
   */
  public static final String FOOTER_KEY_PROPERTY_NAME = "parquet.encryption.footer.key";
  /**
   * Encrypt unlisted columns using footer key.
   * By default, false - unlisted columns are not encrypted.
   */
  public static final String COMPLETE_COLUMN_ENCRYPTION_PROPERTY_NAME = "parquet.encryption.complete.columns";
  /**
   * Master key ID for uniform encryption (same key for all columns and footer).
   */
  public static final String UNIFORM_KEY_PROPERTY_NAME = "parquet.encryption.uniform.key";
  /**
   * Parquet encryption algorithm. Can be "AES_GCM_V1" (default), or "AES_GCM_CTR_V1".
   */
  public static final String ENCRYPTION_ALGORITHM_PROPERTY_NAME = "parquet.encryption.algorithm";
  /**
   * Write files with plaintext footer.
   * By default, false - Parquet footers are encrypted.
   */
  public static final String PLAINTEXT_FOOTER_PROPERTY_NAME = "parquet.encryption.plaintext.footer";

  public static final String ENCRYPTION_ALGORITHM_DEFAULT = ParquetCipher.AES_GCM_V1.toString();
  public static final boolean PLAINTEXT_FOOTER_DEFAULT = false;
  public static final boolean COMPLETE_COLUMN_ENCRYPTION_DEFAULT = false;

  private static final SecureRandom RANDOM = new SecureRandom();


    @Override
  public FileDecryptionProperties getFileDecryptionProperties(TrinoParquetCryptoConfig conf, Location filePath, TrinoFileSystem trinoFileSystem)
      throws ParquetCryptoRuntimeException {

    DecryptionKeyRetriever keyRetriever = new TrinoFileKeyUnwrapper(conf, filePath, trinoFileSystem);

    if (LOG.isDebugEnabled()) {
      LOG.debug("File decryption properties for {}", filePath);
    }

    return FileDecryptionProperties.builder()
        .withKeyRetriever(keyRetriever)
        .withPlaintextFilesAllowed()
        .build();
  }
}
