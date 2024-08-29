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

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;

public class AesCtrDecryptor
        extends AesCipher implements BlockCipher.Decryptor
{
    private final byte[] ctrIV;

    AesCtrDecryptor(byte[] keyBytes)
    {
        super(AesMode.CTR, keyBytes);

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
    public byte[] decrypt(byte[] lengthAndCiphertext, byte[] aad)
    {
        int cipherTextOffset = SIZE_LENGTH;
        int cipherTextLength = lengthAndCiphertext.length - SIZE_LENGTH;

        return decrypt(lengthAndCiphertext, cipherTextOffset, cipherTextLength, aad);
    }

    public byte[] decrypt(byte[] ciphertext, int cipherTextOffset, int cipherTextLength, byte[] aad)
    {
        int plainTextLength = cipherTextLength - NONCE_LENGTH;
        if (plainTextLength < 1) {
            throw new ParquetCryptoRuntimeException("Wrong input length " + plainTextLength);
        }

        // Get the nonce from ciphertext
        System.arraycopy(ciphertext, cipherTextOffset, ctrIV, 0, NONCE_LENGTH);

        byte[] plainText = new byte[plainTextLength];
        int inputLength = cipherTextLength - NONCE_LENGTH;
        int inputOffset = cipherTextOffset + NONCE_LENGTH;
        int outputOffset = 0;
        try {
            IvParameterSpec spec = new IvParameterSpec(ctrIV);
            cipher.init(Cipher.DECRYPT_MODE, aesKey, spec);

            // Breaking decryption into multiple updates, to trigger h/w acceleration in Java 9+
            while (inputLength > CHUNK_LENGTH) {
                int written = cipher.update(ciphertext, inputOffset, CHUNK_LENGTH, plainText, outputOffset);
                inputOffset += CHUNK_LENGTH;
                outputOffset += written;
                inputLength -= CHUNK_LENGTH;
            }

            cipher.doFinal(ciphertext, inputOffset, inputLength, plainText, outputOffset);
        }
        catch (GeneralSecurityException e) {
            throw new ParquetCryptoRuntimeException("Failed to decrypt", e);
        }

        return plainText;
    }

    public ByteBuffer decrypt(ByteBuffer ciphertext, byte[] aad)
    {
        int cipherTextOffset = SIZE_LENGTH;
        int cipherTextLength = ciphertext.limit() - ciphertext.position() - SIZE_LENGTH;

        int plainTextLength = cipherTextLength - NONCE_LENGTH;
        if (plainTextLength < 1) {
            throw new ParquetCryptoRuntimeException("Wrong input length " + plainTextLength);
        }

        // skip size
        ciphertext.position(ciphertext.position() + cipherTextOffset);
        // Get the nonce from ciphertext
        ciphertext.get(ctrIV, 0, NONCE_LENGTH);

        // Reuse the input buffer as the output buffer
        ByteBuffer plainText = ciphertext.slice();
        plainText.limit(plainTextLength);
        int inputLength = cipherTextLength - NONCE_LENGTH;
        int inputOffset = cipherTextOffset + NONCE_LENGTH;
        try {
            IvParameterSpec spec = new IvParameterSpec(ctrIV);
            cipher.init(Cipher.DECRYPT_MODE, aesKey, spec);

            // Breaking decryption into multiple updates, to trigger h/w acceleration in Java 9+
            while (inputLength > CHUNK_LENGTH) {
                ciphertext.position(inputOffset);
                ciphertext.limit(inputOffset + CHUNK_LENGTH);
                cipher.update(ciphertext, plainText);
                inputOffset += CHUNK_LENGTH;
                inputLength -= CHUNK_LENGTH;
            }
            ciphertext.position(inputOffset);
            ciphertext.limit(inputOffset + inputLength);
            cipher.doFinal(ciphertext, plainText);
            plainText.flip();
        }
        catch (GeneralSecurityException e) {
            throw new ParquetCryptoRuntimeException("Failed to decrypt", e);
        }

        return plainText;
    }

    @Override
    public byte[] decrypt(InputStream from, byte[] aad)
            throws IOException
    {
        byte[] lengthBuffer = new byte[SIZE_LENGTH];
        int gotBytes = 0;

        // Read the length of encrypted Thrift structure
        while (gotBytes < SIZE_LENGTH) {
            int n = from.read(lengthBuffer, gotBytes, SIZE_LENGTH - gotBytes);
            if (n <= 0) {
                throw new IOException("Tried to read int (4 bytes), but only got " + gotBytes + " bytes.");
            }
            gotBytes += n;
        }

        final int ciphertextLength = ((lengthBuffer[3] & 0xff) << 24)
                | ((lengthBuffer[2] & 0xff) << 16)
                | ((lengthBuffer[1] & 0xff) << 8)
                | ((lengthBuffer[0] & 0xff));

        if (ciphertextLength < 1) {
            throw new IOException("Wrong length of encrypted metadata: " + ciphertextLength);
        }

        // Read the encrypted structure contents
        byte[] ciphertextBuffer = new byte[ciphertextLength];
        gotBytes = 0;
        while (gotBytes < ciphertextLength) {
            int n = from.read(ciphertextBuffer, gotBytes, ciphertextLength - gotBytes);
            if (n <= 0) {
                throw new IOException(
                        "Tried to read " + ciphertextLength + " bytes, but only got " + gotBytes + " bytes.");
            }
            gotBytes += n;
        }

        // Decrypt the structure contents
        return decrypt(ciphertextBuffer, 0, ciphertextLength, aad);
    }
}
