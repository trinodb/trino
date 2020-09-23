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
package io.trino.client.auth.external;

import java.net.URI;
import java.util.Optional;

import static io.trino.client.auth.external.TokenPoll.Status.FAILED;
import static io.trino.client.auth.external.TokenPoll.Status.PENDING;
import static io.trino.client.auth.external.TokenPoll.Status.SUCCESSFUL;
import static java.util.Objects.requireNonNull;

public class TokenPoll
{
    private final Status status;
    private final Optional<String> errorMessage;
    private final Optional<URI> nextTokenUri;
    private final Optional<AuthenticationToken> token;

    public static TokenPoll failed(String error)
    {
        return new TokenPoll(FAILED, null, error, null);
    }

    public static TokenPoll nextPollingResource(URI uri)
    {
        return new TokenPoll(PENDING, null, null, uri);
    }

    public static TokenPoll successful(AuthenticationToken token)
    {
        return new TokenPoll(SUCCESSFUL, token, null, null);
    }

    private TokenPoll(Status status, AuthenticationToken token, String error, URI nextTokenUri)
    {
        this.status = requireNonNull(status, "status is null");
        this.token = Optional.ofNullable(token);
        this.errorMessage = Optional.ofNullable(error);
        this.nextTokenUri = Optional.ofNullable(nextTokenUri);
    }

    public boolean hasFailed()
    {
        return status == FAILED;
    }

    public boolean isPending()
    {
        return status == PENDING;
    }

    public Optional<AuthenticationToken> getToken()
    {
        return token;
    }

    public Optional<String> getError()
    {
        return errorMessage;
    }

    public Optional<URI> getNextTokenUri()
    {
        return nextTokenUri;
    }

    public enum Status
    {
        PENDING,
        SUCCESSFUL,
        FAILED;
    }
}
