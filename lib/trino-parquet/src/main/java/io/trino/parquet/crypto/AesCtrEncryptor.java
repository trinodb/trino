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
import javax.crypto.spec.IvParameterSpec;

import java.security.GeneralSecurityException;

public class AesCtrEncryptor
        extends AesCipher
        implements BlockCipher.Encryptor
{
    private final byte[] ctrIV;
    private long operationCounter;

    AesCtrEncryptor(byte[] keyBytes)
    {
        super(AesMode.CTR, keyBytes);
        operationCounter = 0;

        try {
            cipher = Cipher.getInstance(AesMode.CTR.getCipherName());
        }
        catch (GeneralSecurityException e) {
            throw new ParquetCryptoRuntimeException("Failed to create CTR cipher", e);
        }

        ctrIV = new byte[CTR_IV_LENGTH];
        // Setting last bit of initial CTR counter to 1
        ctrIV[CTR_IV_LENGTH - 1] = (byte) 1;
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
        if (operationCounter > CTR_RANDOM_IV_SAME_KEY_MAX_OPS) {
            throw new ParquetCryptoRuntimeException(
                    "Exceeded limit of AES CTR encryption operations with same key and random IV");
        }
        operationCounter++;

        if (nonce.length != NONCE_LENGTH) {
            throw new ParquetCryptoRuntimeException("Wrong nonce length " + nonce.length);
        }
        int plainTextLength = plainText.length;
        int cipherTextLength = NONCE_LENGTH + plainTextLength;
        int lengthBufferLength = writeLength ? SIZE_LENGTH : 0;
        byte[] cipherText = new byte[lengthBufferLength + cipherTextLength];
        int inputLength = plainTextLength;
        int inputOffset = 0;
        int outputOffset = lengthBufferLength + NONCE_LENGTH;
        try {
            System.arraycopy(nonce, 0, ctrIV, 0, NONCE_LENGTH);
            IvParameterSpec spec = new IvParameterSpec(ctrIV);
            cipher.init(Cipher.ENCRYPT_MODE, aesKey, spec);

            // Breaking encryption into multiple updates, to trigger h/w acceleration in Java 9+
            while (inputLength > CHUNK_LENGTH) {
                int written = cipher.update(plainText, inputOffset, CHUNK_LENGTH, cipherText, outputOffset);
                inputOffset += CHUNK_LENGTH;
                outputOffset += written;
                inputLength -= CHUNK_LENGTH;
            }

            cipher.doFinal(plainText, inputOffset, inputLength, cipherText, outputOffset);
        }
        catch (GeneralSecurityException e) {
            throw new ParquetCryptoRuntimeException("Failed to encrypt", e);
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
