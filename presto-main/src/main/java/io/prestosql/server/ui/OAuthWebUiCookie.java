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
package io.prestosql.server.ui;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static io.prestosql.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.util.function.Predicate.not;

public final class OAuthWebUiCookie
{
    private static final String OAUTH2_COOKIE = "Presto-OAuth2-Token";

    private OAuthWebUiCookie() {}

    public static NewCookie create(String accessToken, Instant tokenExpiration, boolean secure)
    {
        return new NewCookie(
                OAUTH2_COOKIE,
                accessToken,
                UI_LOCATION,
                null,
                Cookie.DEFAULT_VERSION,
                null,
                NewCookie.DEFAULT_MAX_AGE,
                Date.from(tokenExpiration),
                secure,
                true);
    }

    public static Optional<String> read(ContainerRequestContext request)
    {
        return Optional.ofNullable(request.getCookies().get(OAUTH2_COOKIE))
                .map(Cookie::getValue)
                .filter(not(String::isBlank));
    }

    public static NewCookie delete(boolean secure)
    {
        return new NewCookie(
                OAUTH2_COOKIE,
                "delete",
                UI_LOCATION,
                null,
                Cookie.DEFAULT_VERSION,
                null,
                0,
                null,
                secure,
                true);
    }
}
