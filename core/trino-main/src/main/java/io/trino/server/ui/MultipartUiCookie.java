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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.NewCookie;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static jakarta.ws.rs.core.NewCookie.DEFAULT_MAX_AGE;
import static java.util.Objects.requireNonNull;

public class MultipartUiCookie
{
    // https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis/#section-5.6-3.4.1
    public static final int MAXIMUM_COOKIE_SIZE = 4096;

    private final String cookieName;
    private final Pattern cookiePattern;
    private final String location;

    public MultipartUiCookie(String cookieName, String location)
    {
        this.cookieName = requireNonNull(cookieName, "cookieName is null");
        this.cookiePattern = Pattern.compile("^" + Pattern.quote(cookieName) + "(_\\d+)?$");
        this.location = requireNonNull(location, "location is null");
    }

    public NewCookie[] create(String token, Instant tokenExpiration, boolean isSecure)
    {
        Date expiration = Optional.ofNullable(tokenExpiration).map(Date::from).orElse(null);
        ImmutableList.Builder<NewCookie> cookiesToSet = ImmutableList.builder();
        int index = 0;
        for (String part : splitValueByLength(token)) {
            cookiesToSet.add(new NewCookie.Builder(cookieName(index++))
                    .value(part)
                    .path(location)
                    .domain(null)
                    .comment(null)
                    .maxAge(DEFAULT_MAX_AGE)
                    .expiry(expiration)
                    .secure(isSecure)
                    .httpOnly(true)
                    .build());
        }
        return cookiesToSet.build().toArray(new NewCookie[0]);
    }

    public Optional<String> read(Map<String, Cookie> existingCookies)
    {
        long cookiesCount = existingCookies.values().stream()
                .filter(this::matchesName)
                .filter(cookie -> !isNullOrEmpty(cookie.getValue()))
                .count();

        if (cookiesCount == 0) {
            return Optional.empty();
        }

        StringBuilder token = new StringBuilder();
        for (int i = 0; i < cookiesCount; i++) {
            Cookie cookie = existingCookies.get(cookieName(i));
            if (cookie == null || isNullOrEmpty(cookie.getValue())) {
                return Optional.empty(); // non continuous
            }
            token.append(cookie.getValue());
        }
        return Optional.of(token.toString());
    }

    public NewCookie[] delete(Map<String, Cookie> existingCookies, boolean isSecured)
    {
        ImmutableSet.Builder<NewCookie> cookiesToDelete = ImmutableSet.builder();
        cookiesToDelete.add(deleteCookie(cookieName, isSecured)); // Always invalidate first cookie even if it doesn't exist
        for (Cookie existingCookie : existingCookies.values()) {
            if (matchesName(existingCookie)) {
                cookiesToDelete.add(deleteCookie(existingCookie.getName(), isSecured));
            }
        }
        return cookiesToDelete.build().toArray(new NewCookie[0]);
    }

    private List<String> splitValueByLength(String value)
    {
        return Splitter.fixedLength(maximumCookieValueLength()).splitToList(value);
    }

    private boolean matchesName(Cookie cookie)
    {
        return cookiePattern.matcher(cookie.getName()).matches();
    }

    @VisibleForTesting
    String cookieName(int index)
    {
        if (index == 0) {
            return cookieName;
        }

        return cookieName + '_' + index;
    }

    int maximumCookieValueLength()
    {
        // A browser should be able to accept at least 300 cookies with a maximum size of 4096 bytes, as stipulated by RFC 2109 (#6.3), RFC 2965 (#5.3), and RFC 6265
        return MAXIMUM_COOKIE_SIZE - cookieName(999).length();
    }

    private NewCookie deleteCookie(String name, boolean isSecured)
    {
        return new NewCookie.Builder(name)
                .value("delete")
                .path(location)
                .domain(null)
                .maxAge(0)
                .expiry(null)
                .secure(isSecured)
                .httpOnly(true)
                .build();
    }
}
