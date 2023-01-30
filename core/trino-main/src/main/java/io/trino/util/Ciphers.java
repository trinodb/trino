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
package io.trino.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.security.NoSuchAlgorithmException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class Ciphers
{
    private static final int AES_ENCRYPTION_KEY_BITS = 256;

    private Ciphers() {}

    public static SecretKey createRandomAesEncryptionKey()
    {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(AES_ENCRYPTION_KEY_BITS);
            return keyGenerator.generateKey();
        }
        catch (NoSuchAlgorithmException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to generate new secret key: " + e.getMessage(), e);
        }
    }

    public static Slice serializeAesEncryptionKey(SecretKey key)
    {
        checkArgument(key.getAlgorithm().equals("AES"), "unexpected algorithm: %s", key.getAlgorithm());
        return Slices.wrappedBuffer(key.getEncoded());
    }

    public static SecretKeySpec deserializeAesEncryptionKey(Slice key)
    {
        return new SecretKeySpec(key.byteArray(), key.byteArrayOffset(), key.length(), "AES");
    }

    public static boolean is256BitSecretKeySpec(SecretKey secretKey)
    {
        if (secretKey instanceof SecretKeySpec spec) {
            return spec.getAlgorithm().equals("AES") && spec.getEncoded().length == AES_ENCRYPTION_KEY_BITS / 8;
        }
        return false;
    }
}
