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
package io.trino.server;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

public class InternalCommunicationEncryption
{
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int IV_LENGTH_BYTE = 12;
    private static final int AES_KEY_SIZE_BYTE = 32;
    private static final int TAG_LENGTH_BIT = 128;
    private final InternalCommunicationConfig config;
    private SecretKey secretKey;

    @Inject
    public InternalCommunicationEncryption(InternalCommunicationConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    public boolean isInternalCommunicationEnabled()
    {
        return config.getSharedSecret().isPresent();
    }

    private SecretKey getSecretKey()
    {
        if (this.secretKey == null) {
            try {
                MessageDigest sha = MessageDigest.getInstance("SHA-512");
                String sharedSecret = config.getSharedSecret().orElseThrow(() -> new TrinoException(CONFIGURATION_INVALID, "Missing configuration internal-communication.shared-secret"));
                byte[] secretKey = Base64.getDecoder().decode(sharedSecret);
                this.secretKey = new SecretKeySpec(Arrays.copyOfRange(sha.digest(secretKey), 0, AES_KEY_SIZE_BYTE), "AES");
            }
            catch (NoSuchAlgorithmException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
            }
        }
        return secretKey;
    }

    public byte[] encrypt(byte[] plainText)
    {
        try {
            byte[] iv = new byte[IV_LENGTH_BYTE];
            new SecureRandom().nextBytes(iv);

            GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BIT, iv);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(ENCRYPT_MODE, getSecretKey(), spec);

            byte[] cipherText = cipher.doFinal(plainText);
            ByteBuffer buffer = ByteBuffer.allocate(IV_LENGTH_BYTE + cipherText.length);
            buffer.put(iv);
            buffer.put(cipherText);
            return buffer.array();
        }
        catch (Exception e) {
            throw new TrinoException(CONFIGURATION_INVALID, e);
        }
    }

    public byte[] decrypt(byte[] encrypted)
    {
        ByteBuffer buffer = ByteBuffer.wrap(encrypted);
        byte[] iv = new byte[IV_LENGTH_BYTE];
        buffer.get(iv);
        byte[] cipherText = new byte[buffer.remaining()];
        buffer.get(cipherText);

        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec spec = new GCMParameterSpec(TAG_LENGTH_BIT, iv);
            cipher.init(DECRYPT_MODE, getSecretKey(), spec);
            return cipher.doFinal(cipherText);
        }
        catch (Exception e) {
            throw new TrinoException(CONFIGURATION_INVALID, e);
        }
    }
}
