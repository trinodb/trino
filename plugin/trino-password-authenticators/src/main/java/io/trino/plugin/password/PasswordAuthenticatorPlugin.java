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
package io.trino.plugin.password;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.password.file.FileAuthenticatorFactory;
import io.trino.plugin.password.file.FileGroupProviderFactory;
import io.trino.plugin.password.ldap.LdapAuthenticatorFactory;
import io.trino.plugin.password.salesforce.SalesforceAuthenticatorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.security.GroupProviderFactory;
import io.trino.spi.security.PasswordAuthenticatorFactory;

public class PasswordAuthenticatorPlugin
        implements Plugin
{
    @Override
    public Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories()
    {
        return ImmutableList.<PasswordAuthenticatorFactory>builder()
                .add(new FileAuthenticatorFactory())
                .add(new LdapAuthenticatorFactory())
                .add(new SalesforceAuthenticatorFactory())
                .build();
    }

    @Override
    public Iterable<GroupProviderFactory> getGroupProviderFactories()
    {
        return ImmutableList.of(new FileGroupProviderFactory());
    }
}
