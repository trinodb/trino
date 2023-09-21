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
import io.trino.client.ClientException;
import okhttp3.Authenticator;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;

import javax.security.auth.login.LoginException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Base64;
import java.util.Locale;

import static com.google.common.base.CharMatcher.whitespace;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static com.google.common.net.HttpHeaders.WWW_AUTHENTICATE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

// TODO: This class is similar to SpnegoAuthentication in Airlift. Consider extracting a library.
public class SpnegoHandler
        implements Interceptor, Authenticator
{
    private static final String NEGOTIATE = "Negotiate";
    private final String servicePrincipalPattern;
    private final String remoteServiceName;
    private final boolean useCanonicalHostname;
    private final GSSContextProvider contextProvider;

    public SpnegoHandler(
            String servicePrincipalPattern,
            String remoteServiceName,
            boolean useCanonicalHostname,
            GSSContextProvider contextProvider)
    {
        this.servicePrincipalPattern = requireNonNull(servicePrincipalPattern, "servicePrincipalPattern is null");
        this.remoteServiceName = requireNonNull(remoteServiceName, "remoteServiceName is null");
        this.useCanonicalHostname = useCanonicalHostname;
        this.contextProvider = requireNonNull(contextProvider, "subjectProvider is null");
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
            context = contextProvider.getContext(servicePrincipal);
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
}
