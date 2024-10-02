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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import io.trino.filesystem.encryption.EncryptionKey;

import java.util.Base64;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spooling.filesystem.encryption.HeadersUtils.getOnlyHeader;

public class GcsEncryptionHeadersTranslator
        implements EncryptionHeadersTranslator
{
    @Override
    public EncryptionKey extractKey(Map<String, List<String>> headers)
    {
        byte[] key = Base64.getDecoder().decode(getOnlyHeader(headers, "x-goog-encryption-key"));
        String sha256Checksum = getOnlyHeader(headers, "x-goog-encryption-key-sha256");
        EncryptionKey encryption = new EncryptionKey(key, getOnlyHeader(headers, "x-goog-encryption-algorithm"));
        checkArgument(sha256(encryption).equals(sha256Checksum), "Key SHA256 checksum does not match");
        return encryption;
    }

    @Override
    public Map<String, List<String>> createHeaders(EncryptionKey encryption)
    {
        return ImmutableMap.of(
                "x-goog-encryption-algorithm", ImmutableList.of(encryption.algorithm()),
                "x-goog-encryption-key", ImmutableList.of(encoded(encryption)),
                "x-goog-encryption-key-sha256", ImmutableList.of(sha256(encryption)));
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
