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
package io.prestosql.server.security.oauth2;

import com.github.scribejava.core.model.OAuth2AccessToken;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;

import static java.util.Objects.requireNonNull;

class Challenge
{
    private final State state;
    private final Status status;

    Challenge(State state, Status status)
    {
        this.state = requireNonNull(state, "state is null");
        this.status = requireNonNull(status, "status is null");
    }

    State getState()
    {
        return state;
    }

    Status getStatus()
    {
        return status;
    }

    static class Started
            extends Challenge
    {
        private final String authorizationUrl;

        Started(State state, String authorizationUrl)
        {
            super(state, Status.STARTED);
            this.authorizationUrl = requireNonNull(authorizationUrl, "authorizationUrl is null");
        }

        String getAuthorizationUrl()
        {
            return authorizationUrl;
        }

        Succeeded succeed(OAuth2AccessToken token, Jws<Claims> jwtToken)
        {
            return new Succeeded(getState(), token, jwtToken);
        }

        Failed fail(OAuth2ErrorResponse error)
        {
            return new Failed(getState(), error);
        }
    }

    static class Succeeded
            extends Challenge
    {
        private final OAuth2AccessToken token;
        private final Jws<Claims> jwtToken;

        Succeeded(State state, OAuth2AccessToken token, Jws<Claims> jwtToken)
        {
            super(state, Status.SUCCEEDED);
            this.token = requireNonNull(token, "token is null");
            this.jwtToken = requireNonNull(jwtToken, "jwtToken is null");
        }

        OAuth2AccessToken getToken()
        {
            return token;
        }

        Jws<Claims> getJwtToken()
        {
            return jwtToken;
        }
    }

    static class Failed
            extends Challenge
    {
        private final OAuth2ErrorResponse error;

        Failed(State state, OAuth2ErrorResponse error)
        {
            super(state, Status.FAILED);
            this.error = requireNonNull(error, "error is null");
        }

        OAuth2ErrorResponse getError()
        {
            return error;
        }
    }
}
