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
package io.trino.spi.security;

import static java.util.Objects.requireNonNull;

public final class ExtraCredentials
{
    private static final String INTERNAL_EXTRA_CREDENTIAL_PREFIX = "internal$";
    private static final String AUTHENTICATED_EXTRA_CREDENTIAL_PREFIX = INTERNAL_EXTRA_CREDENTIAL_PREFIX + "authenticated:";

    private ExtraCredentials() {}

    public static boolean isInternalExtraCredential(String name)
    {
        return requireNonNull(name, "name is null").startsWith(INTERNAL_EXTRA_CREDENTIAL_PREFIX);
    }

    public static String authenticatedExtraCredentialName(String name)
    {
        return AUTHENTICATED_EXTRA_CREDENTIAL_PREFIX + requireNonNull(name, "name is null");
    }
}
