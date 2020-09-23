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
package io.prestosql.client.auth.external;

import java.util.Optional;

import static io.prestosql.client.auth.external.TokenPoll.Status.FAILED;
import static io.prestosql.client.auth.external.TokenPoll.Status.PENDING;
import static io.prestosql.client.auth.external.TokenPoll.Status.SUCCESSFUL;
import static java.util.Objects.requireNonNull;

class TokenPoll
{
    private final Status status;
    private final Optional<AuthenticationToken> token;

    static TokenPoll failed()
    {
        return new TokenPoll(FAILED, Optional.empty());
    }

    static TokenPoll pending()
    {
        return new TokenPoll(PENDING, Optional.empty());
    }

    static TokenPoll successful(AuthenticationToken token)
    {
        return new TokenPoll(SUCCESSFUL, Optional.of(token));
    }

    private TokenPoll(Status status, Optional<AuthenticationToken> token)
    {
        this.status = requireNonNull(status, "status is null");
        this.token = requireNonNull(token, "token is null");
    }

    public boolean hasFailed()
    {
        return status == FAILED;
    }

    public boolean isNotAvailableYet()
    {
        return status == PENDING;
    }

    public Optional<AuthenticationToken> getToken()
    {
        return token;
    }

    enum Status
    {
        PENDING,
        SUCCESSFUL,
        FAILED;
    }
}
