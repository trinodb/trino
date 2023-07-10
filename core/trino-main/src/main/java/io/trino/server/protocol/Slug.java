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
package io.trino.server.protocol;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.security.SecureRandom;

import static java.util.Objects.requireNonNull;

public final class Slug
{
    public enum Context
    {
        QUEUED_QUERY,
        EXECUTING_QUERY,
    }

    private static final SecureRandom RANDOM = new SecureRandom();

    public static Slug createNew()
    {
        byte[] randomBytes = new byte[16];
        RANDOM.nextBytes(randomBytes);
        return new Slug(randomBytes);
    }

    private final HashFunction hmac;

    private Slug(byte[] slugKey)
    {
        this.hmac = Hashing.hmacSha1(requireNonNull(slugKey, "slugKey is null"));
    }

    public String makeSlug(Context context, long token)
    {
        // "y" is an arbitrary prefix distinguishing this slug version. Added for troubleshooting purposes.
        return "y" + hmac.newHasher()
                .putInt(context.ordinal())
                .putLong(token)
                .hash()
                .toString();
    }

    public boolean isValid(Context context, String slug, long token)
    {
        return makeSlug(context, token).equals(slug);
    }
}
