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
package io.prestosql.spiller;

import io.prestosql.spi.PrestoException;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

final class AesSpillCipher
        implements SpillCipher
{
    //  256-bit AES CBC mode
    private static final String CIPHER_NAME = "AES/CBC/PKCS5Padding";
    private static final int KEY_BITS = 256;

    private SecretKey key;
    //  Instance used only for determining encrypted output lengths
    private Cipher encryptSizer;
    private final int ivBytes;

    AesSpillCipher()
    {
        this.key = generateNewSecretKey();
        this.encryptSizer = createEncryptCipher(key);
        this.ivBytes = encryptSizer.getIV().length;
    }

    @Override
    public int encryptedMaxLength(int inputLength)
    {
        checkArgument(inputLength >= 0, "inputLength must be >= 0, inputLength=%s", inputLength);
        Cipher sizer = throwCipherClosedIfNull(encryptSizer);
        return ivBytes + sizer.getOutputSize(inputLength);
    }

    @Override
    public int decryptedMaxLength(int encryptedLength)
    {
        checkArgument(encryptedLength >= ivBytes, "encryptedLength must be >= %s", ivBytes);
        return encryptedLength - ivBytes;
    }

    @Override
    public int encrypt(byte[] data, int inputOffset, int length, byte[] destination, int destinationOffset)
    {
        checkArgument(data.length - inputOffset >= length, "data buffer too small for length argument");
        checkArgument(destination.length - destinationOffset >= encryptedMaxLength(length), "destination buffer too small for encrypted output");
        Cipher cipher = createEncryptCipher(key);
        System.arraycopy(cipher.getIV(), 0, destination, destinationOffset, ivBytes);
        try {
            return ivBytes + cipher.doFinal(data, inputOffset, length, destination, destinationOffset + ivBytes);
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to encrypt data: " + e.getMessage(), e);
        }
    }

    @Override
    public int decrypt(byte[] encryptedData, int inputOffset, int length, byte[] destination, int destinationOffset)
    {
        checkArgument(encryptedData.length - inputOffset >= length, "encryptedData too small for length argument");
        checkArgument(destination.length - destinationOffset >= decryptedMaxLength(length), "destination buffer too small for decrypted output");
        Cipher cipher = createDecryptCipher(key, new IvParameterSpec(encryptedData, inputOffset, ivBytes));
        try {
            return cipher.doFinal(encryptedData, inputOffset + ivBytes, length - ivBytes, destination, destinationOffset);
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Cannot decrypt previously encrypted data: " + e.getMessage(), e);
        }
    }

    @Override
    public void close()
    {
        /*
         * Setting the {@link AesSpillCipher#key} to null allows the key to be reclaimed by GC at the time of
         * destruction, even if the {@link AesSpillCipher} itself is still reachable for longer. When the key
         * is null, subsequent encrypt / decrypt operations will fail and can prevent accidental use beyond the
         * intended lifespan of the {@link AesSpillCipher}.
         */
        this.key = null;
        this.encryptSizer = null;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("closed", key == null).toString();
    }

    private static <T> T throwCipherClosedIfNull(T value)
    {
        if (value == null) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Spill cipher already closed");
        }
        return value;
    }

    private static Cipher createEncryptCipher(SecretKey key)
    {
        Cipher cipher = createUninitializedCipher();
        try {
            cipher.init(Cipher.ENCRYPT_MODE, throwCipherClosedIfNull(key));
            return cipher;
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to initialize spill cipher for encryption: " + e.getMessage(), e);
        }
    }

    private static Cipher createDecryptCipher(SecretKey key, IvParameterSpec iv)
    {
        Cipher cipher = createUninitializedCipher();
        try {
            cipher.init(Cipher.DECRYPT_MODE, throwCipherClosedIfNull(key), iv);
            return cipher;
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to initialize spill cipher for decryption: " + e.getMessage(), e);
        }
    }

    private static Cipher createUninitializedCipher()
    {
        try {
            return Cipher.getInstance(CIPHER_NAME);
        }
        catch (GeneralSecurityException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to create spill cipher: " + e.getMessage(), e);
        }
    }

    private static SecretKey generateNewSecretKey()
    {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(KEY_BITS);
            return keyGenerator.generateKey();
        }
        catch (NoSuchAlgorithmException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to generate new secret key: " + e.getMessage(), e);
        }
    }
}
