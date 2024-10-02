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
package io.trino.spooling.filesystem.encryption;

import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import io.trino.filesystem.encryption.EncryptionKey;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spooling.filesystem.encryption.HeadersUtils.getOnlyHeader;

public class AzureEncryptionHeadersTranslator
        implements EncryptionHeadersTranslator
{
    @Override
    public EncryptionKey extractKey(Map<String, List<String>> headers)
    {
        byte[] key = Base64.getDecoder().decode(getOnlyHeader(headers, "x-ms-encryption-key"));
        String sha256Checksum = getOnlyHeader(headers, "x-ms-encryption-key-sha256");
        EncryptionKey encryption = new EncryptionKey(key, getOnlyHeader(headers, "x-ms-encryption-algorithm"));
        checkArgument(sha256(encryption).equals(sha256Checksum), "Key SHA256 checksum does not match");
        return encryption;
    }

    @Override
    public Map<String, List<String>> createHeaders(EncryptionKey key)
    {
        return ImmutableMap.of(
                "x-ms-encryption-key", List.of(encoded(key)),
                "x-ms-encryption-key-sha256", List.of(sha256(key)),
                "x-ms-encryption-algorithm", List.of(key.algorithm()));
    }

    private static String sha256(EncryptionKey key)
    {
        return Base64.getEncoder().encodeToString(Hashing.sha256().hashBytes(key.key()).asBytes());
    }

    private static String encoded(EncryptionKey key)
    {
        return Base64.getEncoder().encodeToString(key.key());
    }
}
