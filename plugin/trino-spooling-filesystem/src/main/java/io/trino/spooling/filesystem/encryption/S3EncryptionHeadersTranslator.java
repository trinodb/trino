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

public class S3EncryptionHeadersTranslator
        implements EncryptionHeadersTranslator
{
    @Override
    public EncryptionKey extractKey(Map<String, List<String>> headers)
    {
        byte[] key = Base64.getDecoder().decode(getOnlyHeader(headers, "x-amz-server-side-encryption-customer-key"));
        String md5Checksum = getOnlyHeader(headers, "x-amz-server-side-encryption-customer-key-MD5");
        EncryptionKey encryption = new EncryptionKey(key, getOnlyHeader(headers, "x-amz-server-side-encryption-customer-algorithm"));
        checkArgument(md5(encryption).equals(md5Checksum), "Key MD5 checksum does not match");
        return encryption;
    }

    @Override
    public Map<String, List<String>> createHeaders(EncryptionKey encryption)
    {
        return ImmutableMap.of(
                "x-amz-server-side-encryption-customer-algorithm", ImmutableList.of(encryption.algorithm()),
                "x-amz-server-side-encryption-customer-key", ImmutableList.of(encoded(encryption)),
                "x-amz-server-side-encryption-customer-key-MD5", ImmutableList.of(md5(encryption)));
    }

    public static String encoded(EncryptionKey key)
    {
        return Base64.getEncoder().encodeToString(key.key());
    }

    @SuppressWarnings("deprecation") // AWS SSE-C requires MD5 checksum
    public static String md5(EncryptionKey key)
    {
        return Base64.getEncoder().encodeToString(Hashing.md5().hashBytes(key.key()).asBytes());
    }
}
