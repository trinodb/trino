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
package io.trino.server.ui;

import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.function.Predicate.not;
import static javax.ws.rs.core.Cookie.DEFAULT_VERSION;
import static javax.ws.rs.core.NewCookie.DEFAULT_MAX_AGE;

public final class OAuthRefreshWebUiCookie
{
    // prefix according to: https://tools.ietf.org/html/draft-ietf-httpbis-rfc6265bis-05#section-4.1.3.1
    public static final String OAUTH2_REFRESH_COOKIE = "__Secure-Trino-OAuth2-Refresh-Token";

    private OAuthRefreshWebUiCookie() {}

    public static NewCookie create(String refreshToken, Instant tokenExpiration)
    {
        return new NewCookie(
                OAUTH2_REFRESH_COOKIE,
                refreshToken,
                UI_LOCATION,
                null,
                DEFAULT_VERSION,
                null,
                DEFAULT_MAX_AGE,
                Date.from(tokenExpiration.plus(5, MINUTES)),
                true,
                true);
    }

    public static Optional<String> read(Cookie cookie)
    {
        return Optional.ofNullable(cookie)
                .map(Cookie::getValue)
                .filter(not(String::isBlank));
    }

    public static NewCookie delete()
    {
        return new NewCookie(
                OAUTH2_REFRESH_COOKIE,
                "delete",
                UI_LOCATION,
                null,
                DEFAULT_VERSION,
                null,
                0,
                null,
                true,
                true);
    }
}
