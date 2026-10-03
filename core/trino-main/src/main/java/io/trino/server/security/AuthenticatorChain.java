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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.security.Identity;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Walks a list of authenticators, accumulating the failures of the ones that reject the request.
 * Keeps the ordering, the suppressed-exception flattening and the error wording in one place,
 * independent of how the caller reads credentials and reports failure.
 */
public final class AuthenticatorChain
{
    private AuthenticatorChain() {}

    /**
     * @param attempt reads credentials for one authenticator, throwing {@link AuthenticationException} when they are rejected
     */
    public static Result authenticate(List<Authenticator> authenticators, AuthenticationAttempt attempt)
    {
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            try {
                return new Authenticated(attempt.authenticate(authenticator));
            }
            catch (AuthenticationException e) {
                // Some authenticators (e.g. password) nest multiple internal authenticators.
                // Exceptions from additional failed login attempts are suppressed in the first exception
                Stream.concat(Stream.of(e), Arrays.stream(e.getSuppressed()))
                        .filter(AuthenticationException.class::isInstance)
                        .map(AuthenticationException.class::cast)
                        .forEach(exception -> {
                            if (exception.getMessage() != null) {
                                messages.add(exception.getMessage());
                            }
                            exception.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                        });
            }
        }

        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }
        // The error is presented to the end user as an exception message, so it must be a single line
        return new Failed(authenticateHeaders, Joiner.on(" | ").join(messages));
    }

    public interface AuthenticationAttempt
    {
        Identity authenticate(Authenticator authenticator)
                throws AuthenticationException;
    }

    public sealed interface Result {}

    public record Authenticated(Identity identity)
            implements Result
    {
        public Authenticated
        {
            requireNonNull(identity, "identity is null");
        }
    }

    public record Failed(Set<String> authenticateHeaders, String error)
            implements Result
    {
        public Failed
        {
            authenticateHeaders = ImmutableSet.copyOf(requireNonNull(authenticateHeaders, "authenticateHeaders is null"));
            requireNonNull(error, "error is null");
        }
    }
}
