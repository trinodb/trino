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
package io.trino.plugin.iceberg;

import com.google.common.primitives.Bytes;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

public final class CredentialsEncryptor
{
    private static final int NONCE_LENGTH = 12;
    private static final int TAG_LENGTH = 128;

    private final SecretKey encryptionKey;

    public CredentialsEncryptor(SecretKey encryptionKey)
    {
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
    }

    public String encrypt(String plaintext)
    {
        try {
            byte[] nonce = new byte[NONCE_LENGTH];
            new SecureRandom().nextBytes(nonce);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(ENCRYPT_MODE, encryptionKey, new GCMParameterSpec(TAG_LENGTH, nonce));
            byte[] encrypted = cipher.doFinal(plaintext.getBytes(UTF_8));

            return Base64.getEncoder().encodeToString(Bytes.concat(nonce, encrypted));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to encrypt", e);
        }
    }

    public String decrypt(String ciphertext)
    {
        try {
            byte[] combined = Base64.getDecoder().decode(ciphertext);
            byte[] nonce = Arrays.copyOfRange(combined, 0, NONCE_LENGTH);
            byte[] encrypted = Arrays.copyOfRange(combined, NONCE_LENGTH, combined.length);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(DECRYPT_MODE, encryptionKey, new GCMParameterSpec(TAG_LENGTH, nonce));
            return new String(cipher.doFinal(encrypted), UTF_8);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to decrypt", e);
        }
    }
}
