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

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.BlockCipher;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import static io.trino.parquet.crypto.AesCipherUtils.GCM_RANDOM_IV_SAME_KEY_MAX_OPS;
import static io.trino.parquet.crypto.AesCipherUtils.GCM_TAG_LENGTH;
import static io.trino.parquet.crypto.AesCipherUtils.GCM_TAG_LENGTH_BITS;
import static io.trino.parquet.crypto.AesCipherUtils.NONCE_LENGTH;
import static io.trino.parquet.crypto.AesCipherUtils.SIZE_LENGTH;
import static io.trino.parquet.crypto.AesCipherUtils.validateKeyBytes;

public class AesGcmEncryptor
        implements BlockCipher.Encryptor
{
    protected Cipher cipher;
    protected final SecureRandom randomGenerator;
    protected final byte[] localNonce;
    protected SecretKeySpec aesKey;

    private long operationCounter;

    public AesGcmEncryptor(byte[] keyBytes)
    {
        validateKeyBytes(keyBytes);
        operationCounter = 0;

        try {
            cipher = Cipher.getInstance(AesMode.GCM.getCipherName());
        }
        catch (GeneralSecurityException e) {
            throw new ParquetCryptoException(e, "Failed to create GCM cipher");
        }

        aesKey = new SecretKeySpec(keyBytes, "AES");
        randomGenerator = new SecureRandom();
        localNonce = new byte[NONCE_LENGTH];
    }

    @Override
    public byte[] encrypt(byte[] plainText, byte[] aad)
    {
        return encrypt(true, plainText, aad);
    }

    public byte[] encrypt(boolean writeLength, byte[] plainText, byte[] aad)
    {
        randomGenerator.nextBytes(localNonce);
        return encrypt(writeLength, plainText, localNonce, aad);
    }

    public byte[] encrypt(boolean writeLength, byte[] plainText, byte[] nonce, byte[] aad)
    {
        if (operationCounter > GCM_RANDOM_IV_SAME_KEY_MAX_OPS) {
            throw new ParquetCryptoException("Exceeded limit of AES GCM encryption operations with same key and random IV");
        }
        operationCounter++;

        if (nonce.length != NONCE_LENGTH) {
            throw new ParquetCryptoException("Wrong nonce length %s", nonce.length);
        }
        int plainTextLength = plainText.length;
        int cipherTextLength = NONCE_LENGTH + plainTextLength + GCM_TAG_LENGTH;
        int lengthBufferLength = writeLength ? SIZE_LENGTH : 0;
        byte[] cipherText = new byte[lengthBufferLength + cipherTextLength];
        int inputOffset = 0;
        int outputOffset = lengthBufferLength + NONCE_LENGTH;

        try {
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH_BITS, nonce);
            cipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);
            if (null != aad) {
                cipher.updateAAD(aad);
            }

            cipher.doFinal(plainText, inputOffset, plainTextLength, cipherText, outputOffset);
        }
        catch (GeneralSecurityException e) {
            throw new ParquetCryptoException(e, "Failed to encrypt");
        }

        // Add ciphertext length
        if (writeLength) {
            System.arraycopy(BytesUtils.intToBytes(cipherTextLength), 0, cipherText, 0, lengthBufferLength);
        }
        // Add the nonce
        System.arraycopy(nonce, 0, cipherText, lengthBufferLength, NONCE_LENGTH);

        return cipherText;
    }
}
