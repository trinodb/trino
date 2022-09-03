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

import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.GlobalSecurityConfig;

import javax.inject.Inject;
import java.util.Optional;

public class ImpersonationCredentialProvider
        implements CredentialProvider
{
    private final boolean isJdbcImpersonationEnabled;
    private final CredentialProvider delegate;
    private final String impersonatingPasswordKey;

    @Inject
    public ImpersonationCredentialProvider(CredentialConfig config, @ForExtraCredentialProvider CredentialProvider delegate)
    {
        this(config.isJdbcImpersonationEnabled(), GlobalSecurityConfig.connectorImpersonatingPasswordKey, delegate);
    }

    private ImpersonationCredentialProvider(boolean isJdbcImpersonationEnabled, String impersonatingPasswordKey, CredentialProvider delegate)
    {
        this.isJdbcImpersonationEnabled = isJdbcImpersonationEnabled;
        this.impersonatingPasswordKey = impersonatingPasswordKey;
        this.delegate = delegate;
    }

    @Override
    public Optional<String> getConnectionUser(Optional<ConnectorIdentity> jdbcIdentity)
    {
        if (isJdbcImpersonationEnabled && jdbcIdentity.isPresent()) {
            return Optional.of(jdbcIdentity.get().getUser());
        }
        return delegate.getConnectionUser(jdbcIdentity);
    }

    @Override
    public Optional<String> getConnectionPassword(Optional<ConnectorIdentity> jdbcIdentity)
    {
        if (isJdbcImpersonationEnabled && jdbcIdentity.isPresent()) {
            return Optional.of(jdbcIdentity.get().getExtraCredentials().get(impersonatingPasswordKey));
        }
        return delegate.getConnectionPassword(jdbcIdentity);
    }
}
