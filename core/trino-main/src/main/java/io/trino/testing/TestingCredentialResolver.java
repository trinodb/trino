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
package io.trino.testing;

import io.trino.spi.security.credential.CredentialProvider;
import io.trino.spi.security.credential.CredentialResolver;

import java.util.Optional;
import java.util.Set;

public class TestingCredentialResolver
        implements CredentialResolver
{
    @Override
    public Optional<CredentialProvider> get(String providerName)
    {
        return Optional.empty();
    }

    @Override
    public Set<String> loadedProviders()
    {
        return Set.of();
    }
}
