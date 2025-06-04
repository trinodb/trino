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

import io.trino.filesystem.encryption.EncryptionKey;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

final class S3SseCUtils
{
    private S3SseCUtils() {}

    public static String encoded(EncryptionKey key)
    {
        return Base64.getEncoder().encodeToString(key.key());
    }

    public static String md5Checksum(EncryptionKey key)
    {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            digest.update(key.key());
            return Base64.getEncoder().encodeToString(digest.digest());
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
