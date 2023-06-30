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
package io.trino.client.auth.kerberos;

import com.google.common.base.Splitter;
import io.airlift.units.Duration;
import io.trino.client.ClientException;
import okhttp3.Authenticator;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import javax.annotation.concurrent.GuardedBy;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.Locale;

import static com.google.common.base.CharMatcher.whitespace;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ietf.jgss.GSSContext.INDEFINITE_LIFETIME;
import static org.ietf.jgss.GSSCredential.DEFAULT_LIFETIME;
import static org.ietf.jgss.GSSCredential.INITIATE_ONLY;
import static org.ietf.jgss.GSSName.NT_HOSTBASED_SERVICE;
import static org.ietf.jgss.GSSName.NT_USER_NAME;

// TODO: This class is similar to SpnegoAuthentication in Airlift. Consider extracting a library.
public class SpnegoHandler
        implements Interceptor, Authenticator
{
    private static final String NEGOTIATE = "Negotiate";
    private static final Duration MIN_CREDENTIAL_LIFETIME = new Duration(60, SECONDS);

    private static final GSSManager GSS_MANAGER = GSSManager.getInstance();

    private static final Oid SPNEGO_OID = createOid("1.3.6.1.5.5.2");
    private static final Oid KERBEROS_OID = createOid("1.2.840.113554.1.2.2");

    private final String servicePrincipalPattern;
    private final String remoteServiceName;
    private final boolean useCanonicalHostname;
    private final SubjectProvider subjectProvider;

    @GuardedBy("this")
    private GSSCredential clientCredential;

    public SpnegoHandler(
            String servicePrincipalPattern,
            String remoteServiceName,
            boolean useCanonicalHostname,
            SubjectProvider subjectProvider)
    {
        this.servicePrincipalPattern = requireNonNull(servicePrincipalPattern, "servicePrincipalPattern is null");
        this.remoteServiceName = requireNonNull(remoteServiceName, "remoteServiceName is null");
        this.useCanonicalHostname = useCanonicalHostname;
        this.subjectProvider = requireNonNull(subjectProvider, "subjectProvider is null");
    }

    @Override
    public Response intercept(Chain chain)
            throws IOException
    {
        // eagerly send authentication if possible
        try {
            return chain.proceed(authenticate(chain.request()));
        }
        catch (ClientException ignored) {
            return chain.proceed(chain.request());
        }
    }

    @Override
    public Request authenticate(Route route, Response response)
    {
        // skip if we already tried or were not asked for Kerberos
        if (response.request().headers(AUTHORIZATION).stream().anyMatch(SpnegoHandler::isNegotiate) ||
                response.headers(WWW_AUTHENTICATE).stream().noneMatch(SpnegoHandler::isNegotiate)) {
            return null;
        }

        return authenticate(response.request());
    }

    private static boolean isNegotiate(String value)
    {
        return Splitter.on(whitespace()).split(value).iterator().next().equalsIgnoreCase(NEGOTIATE);
    }

    private Request authenticate(Request request)
    {
        String hostName = request.url().host();
        String principal = makeServicePrincipal(servicePrincipalPattern, remoteServiceName, hostName, useCanonicalHostname);
        byte[] token = generateToken(principal);

        String credential = NEGOTIATE + " " + Base64.getEncoder().encodeToString(token);
        return request.newBuilder()
                .header(AUTHORIZATION, credential)
                .build();
    }

    private byte[] generateToken(String servicePrincipal)
    {
        GSSContext context = null;
        try {
            GSSCredential clientCredential = getGssCredential();
            context = doAs(subjectProvider.getSubject(), () -> {
                GSSContext result = GSS_MANAGER.createContext(
                        GSS_MANAGER.createName(servicePrincipal, NT_HOSTBASED_SERVICE),
                        SPNEGO_OID,
                        clientCredential,
                        INDEFINITE_LIFETIME);

                result.requestMutualAuth(true);
                result.requestConf(true);
                result.requestInteg(true);
                result.requestCredDeleg(true);
                return result;
            });

            byte[] token = context.initSecContext(new byte[0], 0, 0);
            if (token == null) {
                throw new LoginException("No token generated from GSS context");
            }
            return token;
        }
        catch (GSSException | LoginException e) {
            throw new ClientException(format("Kerberos error for [%s]: %s", servicePrincipal, e.getMessage()), e);
        }
        finally {
            try {
                if (context != null) {
                    context.dispose();
                }
            }
            catch (GSSException ignored) {
            }
        }
    }

    private synchronized GSSCredential getGssCredential()
            throws LoginException, GSSException
    {
        if ((clientCredential == null) || clientCredential.getRemainingLifetime() < MIN_CREDENTIAL_LIFETIME.getValue(SECONDS)) {
            clientCredential = createGssCredential();
        }
        return clientCredential;
    }

    private GSSCredential createGssCredential()
            throws LoginException, GSSException
    {
        subjectProvider.refresh();
        Subject subject = subjectProvider.getSubject();
        Principal clientPrincipal = subject.getPrincipals().iterator().next();
        return doAs(subject, () -> GSS_MANAGER.createCredential(
                GSS_MANAGER.createName(clientPrincipal.getName(), NT_USER_NAME),
                DEFAULT_LIFETIME,
                KERBEROS_OID,
                INITIATE_ONLY));
    }

    private static String makeServicePrincipal(String servicePrincipalPattern, String serviceName, String hostName, boolean useCanonicalHostname)
    {
        String serviceHostName = hostName;
        if (useCanonicalHostname) {
            serviceHostName = canonicalizeServiceHostName(hostName);
        }
        return servicePrincipalPattern.replaceAll("\\$\\{SERVICE}", serviceName).replaceAll("\\$\\{HOST}", serviceHostName.toLowerCase(Locale.US));
    }

    private static String canonicalizeServiceHostName(String hostName)
    {
        try {
            InetAddress address = InetAddress.getByName(hostName);
            String fullHostName;
            if ("localhost".equalsIgnoreCase(address.getHostName())) {
                fullHostName = InetAddress.getLocalHost().getCanonicalHostName();
            }
            else {
                fullHostName = address.getCanonicalHostName();
            }
            if (fullHostName.equalsIgnoreCase("localhost")) {
                throw new ClientException("Fully qualified name of localhost should not resolve to 'localhost'. System configuration error?");
            }
            return fullHostName;
        }
        catch (UnknownHostException e) {
            throw new ClientException("Failed to resolve host: " + hostName, e);
        }
    }

    private interface GssSupplier<T>
    {
        T get()
                throws GSSException;
    }

    private static <T> T doAs(Subject subject, GssSupplier<T> action)
            throws GSSException
    {
        try {
            return Subject.doAs(subject, (PrivilegedExceptionAction<T>) action::get);
        }
        catch (PrivilegedActionException e) {
            Throwable t = e.getCause();
            throwIfInstanceOf(t, GSSException.class);
            throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
    }

    private static Oid createOid(String value)
    {
        try {
            return new Oid(value);
        }
        catch (GSSException e) {
            throw new AssertionError(e);
        }
    }
}
