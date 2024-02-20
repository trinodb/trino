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
package io.trino.server.security;

import com.google.common.base.Splitter;
import jakarta.ws.rs.container.ContainerRequestContext;

import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.net.HttpHeaders.AUTHORIZATION;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;

public class BasicAuthCredentials
{
    public static final String AUTHENTICATE_HEADER = "Basic realm=\"Trino\"";

    private final String user;
    private final Optional<String> password;

    public static Optional<BasicAuthCredentials> extractBasicAuthCredentials(ContainerRequestContext request)
            throws AuthenticationException
    {
        requireNonNull(request, "request is null");

        // This handles HTTP basic auth per RFC 7617. The header contains the
        // case-insensitive "Basic" scheme followed by a Base64 encoded "user:pass".
        String header = nullToEmpty(request.getHeaders().getFirst(AUTHORIZATION));

        return extractBasicAuthCredentials(header);
    }

    public static Optional<BasicAuthCredentials> extractBasicAuthCredentials(String header)
            throws AuthenticationException
    {
        requireNonNull(header, "header is null");

        int space = header.indexOf(' ');
        if ((space < 0) || !header.substring(0, space).equalsIgnoreCase("basic")) {
            return Optional.empty();
        }
        String credentials = decodeCredentials(header.substring(space + 1).trim());

        List<String> parts = Splitter.on(':').limit(2).splitToList(credentials);
        String user = parts.get(0);
        if (user.isEmpty()) {
            throw new AuthenticationException("Malformed credentials: user is empty");
        }
        Optional<String> password = Optional.ofNullable(parts.size() == 2 ? emptyToNull(parts.get(1)) : null);
        return Optional.of(new BasicAuthCredentials(user, password));
    }

    public BasicAuthCredentials(String user, Optional<String> password)
    {
        this.user = requireNonNull(user, "user is null");
        this.password = requireNonNull(password, "password is null");
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getPassword()
    {
        return password;
    }

    private static String decodeCredentials(String credentials)
            throws AuthenticationException
    {
        // The original basic auth RFC 2617 did not specify a character set.
        // Many clients, including the Trino CLI and JDBC driver, use ISO-8859-1.
        // RFC 7617 allows the server to specify UTF-8 as the character set during
        // the challenge, but this doesn't help as most clients pre-authenticate.
        try {
            return new String(Base64.getDecoder().decode(credentials), ISO_8859_1);
        }
        catch (IllegalArgumentException e) {
            throw new AuthenticationException("Invalid base64 encoded credentials");
        }
    }
}
