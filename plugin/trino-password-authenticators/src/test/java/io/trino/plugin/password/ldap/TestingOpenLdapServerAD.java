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
package io.trino.plugin.password.ldap;

import com.google.common.io.Closer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import static io.trino.plugin.password.ldap.LdapUtil.buildLdapGroupObject;
import static io.trino.plugin.password.ldap.LdapUtil.buildLdapUserObject;
import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestingOpenLdapServerAD
        extends TestingOpenLdapServerBase
        implements Closeable
{
    private final Closer closer = Closer.create();

    TestingOpenLdapServerAD(Network network)
    {
        super(network, "ghcr.io/trinodb/testing/centos7-oj17-openldap-active-directory");
        closer.register(openLdapServer::close);
    }

    public DisposableSubContext createGroup(DisposableSubContext organization, String groupMembershipAttribute, String userDN)
            throws Exception
    {
        return createDisposableSubContext(buildLdapGroupObject(organization.getDistinguishedName(), "group_" + randomNameSuffix(), groupMembershipAttribute, userDN));
    }

    public DisposableSubContext createUser(DisposableSubContext organization, String userName, String password, Set<String> groupNames)
            throws Exception
    {
        return createDisposableSubContext(buildLdapUserObject(organization.getDistinguishedName(), userName, password, groupNames));
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }
}
