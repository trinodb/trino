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
package io.trino.plugin.jdbc.credential;

import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExtraCredentialProvider
        implements CredentialProvider
{
    private final Optional<String> userCredentialName;
    private final Optional<String> passwordCredentialName;
    private final CredentialProvider delegate;

    @Inject
    public ExtraCredentialProvider(ExtraCredentialConfig config, @ForExtraCredentialProvider CredentialProvider delegate)
    {
        this(config.getUserCredentialName(), config.getPasswordCredentialName(), delegate);
    }

    public ExtraCredentialProvider(Optional<String> userCredentialName, Optional<String> passwordCredentialName, CredentialProvider delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.userCredentialName = requireNonNull(userCredentialName, "userCredentialName is null");
        this.passwordCredentialName = requireNonNull(passwordCredentialName, "passwordCredentialName is null");
    }

    @Override
    public Optional<String> getConnectionUser(Optional<ConnectorIdentity> jdbcIdentity)
    {
        if (jdbcIdentity.isPresent() && userCredentialName.isPresent()) {
            Map<String, String> extraCredentials = jdbcIdentity.get().getExtraCredentials();
            if (extraCredentials.containsKey(userCredentialName.get())) {
                return Optional.of(extraCredentials.get(userCredentialName.get()));
            }
        }
        return delegate.getConnectionUser(jdbcIdentity);
    }

    @Override
    public Optional<String> getConnectionPassword(Optional<ConnectorIdentity> jdbcIdentity)
    {
        if (jdbcIdentity.isPresent() && passwordCredentialName.isPresent()) {
            Map<String, String> extraCredentials = jdbcIdentity.get().getExtraCredentials();
            if (extraCredentials.containsKey(passwordCredentialName.get())) {
                return Optional.of(extraCredentials.get(passwordCredentialName.get()));
            }
        }
        return delegate.getConnectionPassword(jdbcIdentity);
    }
}
