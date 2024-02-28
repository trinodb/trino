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

import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.NewCookie;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;

public final class OAuthWebUiCookie
{
    // prefix according to: https://tools.ietf.org/html/draft-ietf-httpbis-rfc6265bis-05#section-4.1.3.1
    public static final String OAUTH2_COOKIE = "__Secure-Trino-OAuth2-Token";
    private static final MultipartUiCookie MULTIPART_COOKIE = new MultipartUiCookie(OAUTH2_COOKIE, UI_LOCATION);

    private OAuthWebUiCookie() {}

    public static NewCookie[] create(String token, Instant tokenExpiration)
    {
        return MULTIPART_COOKIE.create(token, tokenExpiration, true);
    }

    public static Optional<String> read(Map<String, Cookie> availableCookies)
    {
        return MULTIPART_COOKIE.read(availableCookies);
    }

    public static NewCookie[] delete(Map<String, Cookie> availableCookies)
    {
        return MULTIPART_COOKIE.delete(availableCookies, true);
    }
}
