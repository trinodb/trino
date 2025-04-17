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

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;

import static com.google.common.primitives.Bytes.concat;
import static java.util.Objects.requireNonNull;

public class AesCipher
{
    public static final int SIZE_LENGTH = 4;
    public static final int NONCE_LENGTH = 12;
    public static final int GCM_TAG_LENGTH = 16;
    protected static final int CTR_IV_LENGTH = 16;
    protected static final int GCM_TAG_LENGTH_BITS = 8 * GCM_TAG_LENGTH;
    protected static final int CHUNK_LENGTH = 4 * 1024;
    // NIST SP 800-38D section 8.3 specifies limit on AES GCM encryption operations with same key and random IV/nonce
    protected static final long GCM_RANDOM_IV_SAME_KEY_MAX_OPS = 1L << 32;
    // NIST SP 800-38A doesn't specify limit on AES CTR operations.
    // However, Parquet uses a random IV (with 12-byte random nonce). To avoid repetition due to "birthday problem",
    // setting a conservative limit equal to GCM's value for random IVs
    protected static final long CTR_RANDOM_IV_SAME_KEY_MAX_OPS = GCM_RANDOM_IV_SAME_KEY_MAX_OPS;

    protected final SecureRandom randomGenerator;
    protected final byte[] localNonce;
    protected SecretKeySpec aesKey;
    protected Cipher cipher;

    AesCipher(byte[] keyBytes)
    {
        requireNonNull(keyBytes, "key bytes cannot be null");
        boolean allZeroKey = true;
        for (byte kb : keyBytes) {
            if (kb != 0) {
                allZeroKey = false;
                break;
            }
        }

        if (allZeroKey) {
            throw new IllegalArgumentException("All key bytes are zero");
        }

        aesKey = new SecretKeySpec(keyBytes, "AES");
        randomGenerator = new SecureRandom();
        localNonce = new byte[NONCE_LENGTH];
    }

    public static byte[] createModuleAAD(byte[] fileAAD, ModuleType moduleType, int rowGroupOrdinal, int columnOrdinal, int pageOrdinal)
    {
        byte[] typeOrdinalBytes = new byte[1];
        typeOrdinalBytes[0] = moduleType.getValue();

        if (ModuleType.Footer == moduleType) {
            return concat(fileAAD, typeOrdinalBytes);
        }

        if (rowGroupOrdinal < 0) {
            throw new IllegalArgumentException("Wrong row group ordinal: " + rowGroupOrdinal);
        }
        short shortRGOrdinal = (short) rowGroupOrdinal;
        if (shortRGOrdinal != rowGroupOrdinal) {
            throw new ParquetCryptoException("Encrypted parquet files can't have more than %s row groups: %s", Short.MAX_VALUE, rowGroupOrdinal);
        }
        byte[] rowGroupOrdinalBytes = shortToBytesLittleEndian(shortRGOrdinal);

        if (columnOrdinal < 0) {
            throw new IllegalArgumentException("Wrong column ordinal: " + columnOrdinal);
        }
        short shortColumOrdinal = (short) columnOrdinal;
        if (shortColumOrdinal != columnOrdinal) {
            throw new ParquetCryptoException("Encrypted parquet files can't have more than %s columns: %s", Short.MAX_VALUE, columnOrdinal);
        }
        byte[] columnOrdinalBytes = shortToBytesLittleEndian(shortColumOrdinal);

        if (ModuleType.DataPage != moduleType && ModuleType.DataPageHeader != moduleType) {
            return concat(fileAAD, typeOrdinalBytes, rowGroupOrdinalBytes, columnOrdinalBytes);
        }

        if (pageOrdinal < 0) {
            throw new IllegalArgumentException("Wrong page ordinal: " + pageOrdinal);
        }
        short shortPageOrdinal = (short) pageOrdinal;
        if (shortPageOrdinal != pageOrdinal) {
            throw new ParquetCryptoException("Encrypted parquet files can't have more than %s pages per chunk: %s", Short.MAX_VALUE, pageOrdinal);
        }
        byte[] pageOrdinalBytes = shortToBytesLittleEndian(shortPageOrdinal);

        return concat(fileAAD, typeOrdinalBytes, rowGroupOrdinalBytes, columnOrdinalBytes, pageOrdinalBytes);
    }

    public static byte[] createFooterAAD(byte[] aadPrefixBytes)
    {
        return createModuleAAD(aadPrefixBytes, ModuleType.Footer, -1, -1, -1);
    }

    // Update last two bytes with new page ordinal (instead of creating new page AAD from scratch)
    public static void quickUpdatePageAAD(byte[] pageAAD, int newPageOrdinal)
    {
        requireNonNull(pageAAD, "pageAAD cannot be null");
        if (newPageOrdinal < 0) {
            throw new IllegalArgumentException("Wrong page ordinal: " + newPageOrdinal);
        }
        short shortPageOrdinal = (short) newPageOrdinal;
        if (shortPageOrdinal != newPageOrdinal) {
            throw new ParquetCryptoException("Encrypted parquet files can't have more than %s pages per chunk: %s", Short.MAX_VALUE, newPageOrdinal);
        }

        byte[] pageOrdinalBytes = shortToBytesLittleEndian(shortPageOrdinal);
        System.arraycopy(pageOrdinalBytes, 0, pageAAD, pageAAD.length - 2, 2);
    }

    protected static int readCiphertextLength(InputStream from)
            throws IOException
    {
        byte[] lengthBuffer = new byte[SIZE_LENGTH];
        int readBytes = 0;

        // Read the length of encrypted Thrift structure
        while (readBytes < SIZE_LENGTH) {
            int n = from.read(lengthBuffer, readBytes, SIZE_LENGTH - readBytes);
            if (n <= 0) {
                throw new IOException("Tried to read int (4 bytes), but only got " + readBytes + " bytes.");
            }
            readBytes += n;
        }

        int ciphertextLength = ((lengthBuffer[3] & 0xff) << 24)
                | ((lengthBuffer[2] & 0xff) << 16)
                | ((lengthBuffer[1] & 0xff) << 8)
                | (lengthBuffer[0] & 0xff);

        if (ciphertextLength < 1) {
            throw new IOException("Wrong length of encrypted metadata: " + ciphertextLength);
        }

        return ciphertextLength;
    }

    private static byte[] shortToBytesLittleEndian(short input)
    {
        byte[] output = new byte[2];
        output[1] = (byte) (0xff & (input >> 8));
        output[0] = (byte) (0xff & input);

        return output;
    }
}
