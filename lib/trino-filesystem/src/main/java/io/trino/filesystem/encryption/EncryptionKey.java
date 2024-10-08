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
package io.trino.filesystem.encryption;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

public record EncryptionKey(byte[] key, String algorithm)
{
    public EncryptionKey
    {
        requireNonNull(algorithm, "algorithm is null");
        requireNonNull(key, "key is null");
    }

    public static EncryptionKey randomAes256()
    {
        byte[] key = new byte[32];
        ThreadLocalRandom.current().nextBytes(key);
        return new EncryptionKey(key, "AES256");
    }

    @Override
    public String toString()
    {
        // We intentionally overwrite toString to hide a key
        return algorithm;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (!(o instanceof EncryptionKey that)) {
            return false;
        }
        return Objects.deepEquals(key, that.key)
                && Objects.equals(algorithm, that.algorithm);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(Arrays.hashCode(key), algorithm);
    }
}
