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
package io.trino.plugin.credential.basic;

import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.credential.BasicCredential;
import io.trino.spi.security.credential.Credential;
import io.trino.spi.security.credential.CredentialProvider;

import static java.util.Objects.requireNonNull;

public class BasicCredentialProvider
        implements CredentialProvider
{
    private final BasicCredential credential;

    @Inject
    public BasicCredentialProvider(BasicCredentialProviderConfig config)
    {
        requireNonNull(config, "config is null");
        credential = new BasicCredential(config.getUsername(), config.getPassword());
    }

    @Override
    public <T extends Credential> T getCredential(ConnectorIdentity identity, Class<T> type)
    {
        assertSupportedTypes(type, BasicCredential.class);
        return (T) credential;
    }
}
