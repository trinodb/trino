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
package io.trino.testing.containers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.testing.TestingNames;
import io.trino.testing.TestingProperties;
import io.trino.testing.containers.ldap.LdapObjectDefinition;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static io.trino.testing.containers.ldap.LdapUtil.MEMBER;
import static io.trino.testing.containers.ldap.LdapUtil.addAttributesToExistingLdapObjects;
import static io.trino.testing.containers.ldap.LdapUtil.addLdapDefinition;
import static io.trino.testing.containers.ldap.LdapUtil.buildLdapGroupObject;
import static io.trino.testing.containers.ldap.LdapUtil.buildLdapOrganizationObject;
import static io.trino.testing.containers.ldap.LdapUtil.buildLdapUserObject;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingOpenLdapServer
        implements Closeable
{
    private static final String BASE_DISTINGUISED_NAME = "dc=trino,dc=testldap,dc=com";

    public static final int LDAP_PORT = 389;

    private final Closer closer = Closer.create();
    private final GenericContainer<?> openLdapServer;

    public TestingOpenLdapServer(Network network)
    {
        openLdapServer = new GenericContainer<>("ghcr.io/trinodb/testing/almalinux9-oj17-openldap:" + TestingProperties.getDockerImagesVersion())
                .withNetwork(network)
                .withExposedPorts(LDAP_PORT)
                .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                .waitingFor(new HostPortWaitStrategy())
                .withStartupTimeout(Duration.ofMinutes(5));

        closer.register(openLdapServer::close);
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

    public DisposableSubContext createOrganization()
            throws NamingException
    {
        return createOrganization("organization_" + TestingNames.randomNameSuffix());
    }

    public DisposableSubContext createOrganization(String name)
            throws NamingException
    {
        return createOrganization(name, new DisposableSubContext(BASE_DISTINGUISED_NAME));
    }

    public DisposableSubContext createOrganization(String name, DisposableSubContext subOrganization)
            throws NamingException
    {
        return createDisposableSubContext(buildLdapOrganizationObject(name, subOrganization.getDistinguishedName()));
    }

    public DisposableSubContext createGroup(DisposableSubContext organization)
            throws Exception
    {
        return createGroup(organization, "group_" + TestingNames.randomNameSuffix());
    }

    public DisposableSubContext createGroup(DisposableSubContext organization, String name)
            throws Exception
    {
        return createDisposableSubContext(buildLdapGroupObject(organization.getDistinguishedName(), name));
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
            return new DisposableSubContext(addLdapDefinition(ldapObjectDefinition, context));
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

    private DirContext createContext()
    {
        Map<String, String> environment = ImmutableMap.<String, String>builder()
                .put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory")
                .put(Context.PROVIDER_URL, getLdapUrl())
                .put(Context.SECURITY_AUTHENTICATION, "simple")
                .put(Context.SECURITY_PRINCIPAL, "cn=admin,dc=trino,dc=testldap,dc=com")
                .put(Context.SECURITY_CREDENTIALS, "admin")
                .buildOrThrow();
        try {
            Properties properties = new Properties();
            properties.putAll(environment);
            return new InitialDirContext(properties);
        }
        catch (NamingException e) {
            throw new RuntimeException("Connection to LDAP server failed", e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    public class DisposableSubContext
            implements AutoCloseable
    {
        private final String distinguishedName;

        public DisposableSubContext(String distinguishedName)
        {
            this.distinguishedName = requireNonNull(distinguishedName, "distinguishedName is null");
        }

        public String getDistinguishedName()
        {
            return distinguishedName;
        }

        @Override
        public void close()
                throws Exception
        {
            DirContext context = createContext();
            context.destroySubcontext(distinguishedName);
            context.close();
        }
    }
}
