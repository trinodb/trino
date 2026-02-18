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
package io.trino.plugin.iceberg.encryption;

import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Map;

public class TestingKmsClient
        implements KeyManagementClient
{
    private final SecureRandom secureRandom = new SecureRandom();

    @Override
    public void initialize(Map<String, String> properties)
    {
        // No-op for testing.
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String keyId)
    {
        return copy(key);
    }

    @Override
    public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String keyId)
    {
        return copy(wrappedKey);
    }

    @Override
    public boolean supportsKeyGeneration()
    {
        return true;
    }

    @Override
    public KeyGenerationResult generateKey(String keyId)
    {
        byte[] key = new byte[32];
        secureRandom.nextBytes(key);
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);
        return new KeyGenerationResult(keyBuffer, copy(keyBuffer));
    }

    private static ByteBuffer copy(ByteBuffer buffer)
    {
        return ByteBuffer.wrap(ByteBuffers.toByteArray(buffer));
    }
}
