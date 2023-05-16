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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.TestingProperties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;

import java.time.Duration;
import java.util.Map;

import static io.trino.plugin.base.jndi.JndiUtils.createDirContext;
import static io.trino.plugin.password.ldap.LdapUtil.MEMBER;
import static io.trino.plugin.password.ldap.LdapUtil.addAttributesToExistingLdapObjects;
import static io.trino.plugin.password.ldap.LdapUtil.addLdapDefinition;
import static io.trino.plugin.password.ldap.LdapUtil.buildLdapGroupObject;
import static io.trino.plugin.password.ldap.LdapUtil.buildLdapOrganizationObject;
import static io.trino.plugin.password.ldap.LdapUtil.buildLdapUserObject;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestingOpenLdapServerBase
{
    protected static final String BASE_DISTINGUISED_NAME = "dc=trino,dc=testldap,dc=com";

    protected static final int LDAP_PORT = 389;

    protected final GenericContainer<?> openLdapServer;
    private String image;

    public TestingOpenLdapServerBase(Network network, String image)
    {
        this.image = image;
        openLdapServer = new GenericContainer<>(this.image + ":" + TestingProperties.getDockerImagesVersion())
                .withNetwork(network)
                .withExposedPorts(LDAP_PORT)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy())
                .withStartupTimeout(Duration.ofMinutes(5));
    }

    public void start()
    {
        openLdapServer.start();
    }

    public String getNetworkAlias()
    {
        return openLdapServer.getNetworkAliases().get(0);
    }

    public String getLdapUrl()
    {
        return format("ldap://%s:%s", openLdapServer.getHost(), openLdapServer.getMappedPort(LDAP_PORT));
    }

    protected DirContext createContext()
    {
        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, getLdapUrl())
                .put(Context.SECURITY_AUTHENTICATION, "simple")
                .put(Context.SECURITY_PRINCIPAL, "cn=admin,dc=trino,dc=testldap,dc=com")
                .put(Context.SECURITY_CREDENTIALS, "admin")
                .buildOrThrow();
        try {
            return createDirContext(environment);
        }
        catch (NamingException e) {
            throw new RuntimeException("Connection to LDAP server failed", e);
        }
    }

    public DisposableSubContext createOrganization()
            throws NamingException
    {
        return createDisposableSubContext(buildLdapOrganizationObject("organization_" + randomNameSuffix(), BASE_DISTINGUISED_NAME));
    }

    public DisposableSubContext createGroup(DisposableSubContext organization)
            throws Exception
    {
        return createDisposableSubContext(buildLdapGroupObject(organization.getDistinguishedName(), "group_" + randomNameSuffix()));
    }

    public DisposableSubContext createUser(DisposableSubContext organization, String userName, String password)
            throws Exception
    {
        return createDisposableSubContext(buildLdapUserObject(organization.getDistinguishedName(), userName, password));
    }

    public DisposableSubContext createDisposableSubContext(LdapObjectDefinition ldapObjectDefinition)
            throws NamingException
    {
        DirContext context = createContext();
        try {
            return new DisposableSubContext(addLdapDefinition(ldapObjectDefinition, context), this);
        }
        finally {
            context.close();
        }
    }

    public void addUserToGroup(DisposableSubContext userDistinguishedName, DisposableSubContext groupDistinguishedName)
            throws Exception
    {
        DirContext context = createContext();
        try {
            addAttributesToExistingLdapObjects(
                    groupDistinguishedName.getDistinguishedName(),
                    ImmutableMap.of(MEMBER, ImmutableList.of(userDistinguishedName.getDistinguishedName())),
                    context);
        }
        finally {
            context.close();
        }
    }
}
