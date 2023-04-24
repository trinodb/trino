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
package io.trino.filesystem;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.filesystem.ParsedLocation.parseLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestParsedLocation
{
    @Test
    void testParse()
    {
        assertParsedLocation("scheme://userInfo@host/some/path", "scheme", Optional.of("userInfo"), "host", "some/path");
        // fragment and query string are ignored
        assertParsedLocation("scheme://userInfo@host/some/path#fragement @ anything allowed", "scheme", Optional.of("userInfo"), "host", "some/path");
        assertParsedLocation("scheme://userInfo@host/some/path?query @ anything allowed", "scheme", Optional.of("userInfo"), "host", "some/path");
        assertParsedLocation("scheme://userInfo@host/some/path#fragement?query", "scheme", Optional.of("userInfo"), "host", "some/path");
        assertParsedLocation("scheme://userInfo@host/some/path?query#fragement", "scheme", Optional.of("userInfo"), "host", "some/path");
        // case is preserved
        assertParsedLocation("SCHEME://USER_INFO@HOST/SOME/PATH#FRAGMENT", "SCHEME", Optional.of("USER_INFO"), "HOST", "SOME/PATH");
        // whitespace is allowed
        assertParsedLocation("sc heme://user info@ho st/so me/pa th#fragment", "sc heme", Optional.of("user info"), "ho st", "so me/pa th");

        // userInfo is optional
        assertParsedLocation("scheme://host/some/path", "scheme", Optional.empty(), "host", "some/path");
        // userInfo can be empty string
        assertParsedLocation("scheme://@host/some/path", "scheme", Optional.of(""), "host", "some/path");

        // host can be empty string
        assertParsedLocation("scheme:///some/path", "scheme", Optional.empty(), "", "some/path");
        // host and userInfo can both be empty
        assertParsedLocation("scheme://@/some/path", "scheme", Optional.of(""), "", "some/path");

        // path can contain anything
        assertParsedLocation("scheme://host/..", "scheme", Optional.empty(), "host", "..");

        assertParsedLocation("scheme://host/path/../../other", "scheme", Optional.empty(), "host", "path/../../other");

        assertParsedLocation("scheme://host/path/%41%illegal", "scheme", Optional.empty(), "host", "path/%41%illegal");

        assertParsedLocation("scheme://host///path", "scheme", Optional.empty(), "host", "//path");

        assertParsedLocation("scheme://host///path//", "scheme", Optional.empty(), "host", "//path//");

        // the path can be empty
        assertParsedLocation("scheme://host", "scheme", Optional.empty(), "host", "");
        assertParsedLocation("scheme://", "scheme", Optional.empty(), "", "");
        assertParsedLocation("scheme://host/", "scheme", Optional.empty(), "host", "");
        assertParsedLocation("scheme:///", "scheme", Optional.empty(), "", "");
        assertParsedLocation("scheme://host?query#fragment", "scheme", Optional.empty(), "host", "");

        assertParsedLocation("scheme://host#fragment?query", "scheme", Optional.empty(), "host", "");

        assertParsedLocation("scheme://host?/ignored", "scheme", Optional.empty(), "host", "");

        // the path can be just a slash (if you really want)
        assertParsedLocation("scheme://host//", "scheme", Optional.empty(), "host", "/");
        assertParsedLocation("scheme:////", "scheme", Optional.empty(), "", "/");

        assertThatThrownBy(() -> parseLocation(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> parseLocation(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheme");
    }

    private static void assertParsedLocation(String location, String scheme, Optional<String> userInfo, String host, String path)
    {
        ParsedLocation parsedLocation = parseLocation(location);
        assertThat(parsedLocation.scheme()).isEqualTo(scheme);
        assertThat(parsedLocation.userInfo()).isEqualTo(userInfo);
        assertThat(parsedLocation.host()).isEqualTo(host);
        assertThat(parsedLocation.path()).isEqualTo(path);
    }
}
