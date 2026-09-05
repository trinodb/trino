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
package io.trino.spi.security.credential;

import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

public interface CredentialProvider
{
    <T extends Credential> T getCredential(ConnectorIdentity identity, Class<T> type);

    default void assertSupportedTypes(Class<? extends Credential> type, Class<? extends Credential>... supportedTypes)
    {
        for (Class<? extends Credential> supportedType : supportedTypes) {
            if (type.isAssignableFrom(supportedType)) {
                return;
            }
        }

        throw new TrinoException(CONFIGURATION_INVALID, "Configured %s does not return %s but only one of: [%s]".formatted(
                getClass().getSimpleName(),
                type.getSimpleName(),
                stream(supportedTypes).map(Class::getSimpleName).collect(joining(", "))));
    }
}
