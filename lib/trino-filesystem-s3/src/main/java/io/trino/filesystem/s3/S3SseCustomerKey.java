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
package io.trino.filesystem.s3;

import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.utils.BinaryUtils.fromBase64;
import static software.amazon.awssdk.utils.Md5Utils.md5AsBase64;

public record S3SseCustomerKey(String key, String md5, String algorithm)
{
    private static final String SSE_C_ALGORITHM = "AES256";

    public S3SseCustomerKey
    {
        requireNonNull(key, "key is null");
        requireNonNull(md5, "md5 is null");
        requireNonNull(algorithm, "algorithm is null");
    }

    public static S3SseCustomerKey onAes256(String key)
    {
        return new S3SseCustomerKey(key, md5AsBase64(fromBase64(key)), SSE_C_ALGORITHM);
    }
}
