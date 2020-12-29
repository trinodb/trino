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
package io.prestosql.client;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.util.Objects.requireNonNull;

public final class KerberosUtil
{
    private static final String CNAME = "CNAME";
    private static final String A_RECORD = "A";
    private static final String FILE_PREFIX = "FILE:";

    private KerberosUtil() {}

    public static Optional<String> defaultCredentialCachePath()
    {
        String value = nullToEmpty(System.getenv("KRB5CCNAME"));
        if (value.startsWith(FILE_PREFIX)) {
            value = value.substring(FILE_PREFIX.length());
        }
        return Optional.ofNullable(emptyToNull(value));
    }

    /**
     * This method resolves the CName pointing at DC hostname (A record) for SPN.
     * If host example.com is mapped with proxy.com (example.com --> proxy.com --> 10.54.250.180)
     * then proxy.com will be returned as service name.
     * If there is a chain of aliases (example.com --> example2.com --> example3.com -->proxy.com -->10.54.250.180
     * proxy.com will be returned as service name.
     *
     * @param host host name to be validated.
     * @return serviceName
     */
    public static String resolve(String host)
    {
        requireNonNull(host, "hostname is null");
        try {
            DirContext ictx = createDefaultDirContext();
            return resolveInternal(host, ictx);
        }
        catch (ClientException ignored) {
        }
        return host;
    }

    static String resolve(String host, DirContext ictx)
    {
        requireNonNull(host, "hostname is null");
        try {
            return resolveInternal(host, ictx);
        }
        catch (ClientException ignored) {
        }
        return host;
    }

    private static String resolveInternal(String hostName, DirContext ictx)
    {
        try {
            Attributes attributes = ictx.getAttributes(hostName, new String[]{CNAME, A_RECORD});
            NamingEnumeration<? extends Attribute> namingEnumeration = attributes.getAll();

            if (namingEnumeration.hasMore()) {
                Attribute attribute = namingEnumeration.next();
                String attributeId = attribute.getID();
                switch (attributeId) {
                    case CNAME:
                        return resolveInternal(attribute.get().toString(), ictx);
                    case A_RECORD:
                        return hostName.endsWith(".") ? hostName.substring(0, hostName.length() - 1) : hostName;
                    default: //do nothing
                        break;
                }
            }
        }
        catch (NamingException e) {
            throw new ClientException("Failed to resolve host: " + hostName, e);
        }
        throw new ClientException("Failed to resolve host: " + hostName);
    }

    private static DirContext createDefaultDirContext()
    {
        Properties environment = new Properties();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        environment.put(Context.PROVIDER_URL, "dns:");
        DirContext ictx;
        try {
            ictx = new InitialDirContext(environment);
        }
        catch (NamingException e) {
            // Please suggest if any logging can be added here
            throw new ClientException("Failed to create initial context for DNS resolution");
        }
        return ictx;
    }
}
