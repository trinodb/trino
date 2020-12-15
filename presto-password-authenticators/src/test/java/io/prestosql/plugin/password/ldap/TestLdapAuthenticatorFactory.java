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
package io.prestosql.plugin.password.ldap;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestLdapAuthenticatorFactory
{
    @Test
    public void testEnvironmentVariablesUsage()
    {
        String user = System.getenv("USER");
        assertThat(user).isNotNull();

        LdapAuthenticatorFactory factory = new LdapAuthenticatorFactory();
        LdapConfig config = factory.create(
                ImmutableMap.<String, String>builder()
                        .put("ldap.url", "ldaps://url")
                        .put("ldap.group-auth-pattern", "")
                        .put("ldap.user-base-dn", "")
                        .put("ldap.bind-dn", "${ENV:USER}")
                        .put("ldap.bind-password", "${ENV:USER}")
                        .build(),
                LdapConfig.class);
        assertThat(config.getBindDistingushedName()).isEqualTo(user);
        assertThat(config.getBindPassword()).isEqualTo(user);
    }
}
