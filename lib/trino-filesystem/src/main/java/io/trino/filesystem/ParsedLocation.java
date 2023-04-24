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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Character.isWhitespace;
import static java.util.Objects.requireNonNull;

/**
 * Parse URI {@code scheme://[userInfo@]host[:port][/path]}
 */
public record ParsedLocation(String location, String scheme, Optional<String> userInfo, String host, String path)
{
    private static final Splitter SCHEME_SPLITTER = Splitter.on("://").limit(2);
    private static final Splitter QUERY_FRAGMENT_SPLITTER = Splitter.on(CharMatcher.anyOf("?#")).limit(2);
    private static final Splitter USER_INFO_SPLITTER = Splitter.on('@').limit(2);
    private static final Splitter AUTHORITY_SPLITTER = Splitter.on('/').limit(2);
    private static final Splitter HOST_AND_PORT_SPLITTER = Splitter.on(':').limit(2);

    public static ParsedLocation parseLocation(String location)
    {
        requireNonNull(location, "location is null");

        List<String> schemeSplit = SCHEME_SPLITTER.splitToList(location);
        checkArgument(schemeSplit.size() == 2, "No scheme for location: %s", location);
        String scheme = schemeSplit.get(0);

        // Fragment is always ignored
        String afterScheme = QUERY_FRAGMENT_SPLITTER.split(schemeSplit.get(1)).iterator().next();

        List<String> userInfoSplit = USER_INFO_SPLITTER.splitToList(afterScheme);
        Optional<String> userInfo = userInfoSplit.size() == 2 ? Optional.of(userInfoSplit.get(0)) : Optional.empty();

        List<String> authoritySplit = AUTHORITY_SPLITTER.splitToList(Iterables.getLast(userInfoSplit));
        String host = authoritySplit.get(0);
        checkArgument(HOST_AND_PORT_SPLITTER.splitToStream(host).count() == 1, "Port is not allowed for location: %s", location);

        String path = (authoritySplit.size() == 2) ? authoritySplit.get(1) : "";

        return new ParsedLocation(location, scheme, userInfo, host, path);
    }

    public ParsedLocation
    {
        requireNonNull(scheme, "scheme is null");
        requireNonNull(userInfo, "userInfo is null");
        requireNonNull(host, "host is null");
        requireNonNull(path, "path is null");
    }

    public void verifyValidFileLocation()
    {
        // file path must not be empty
        checkArgument(!path.isEmpty() && !path.equals("/"), "File location must contain a path: %s", location);
        // file path cannot end with a slash
        checkArgument(!path.endsWith("/"), "File location cannot end with '/': %s", location);
        // file path cannot end with whitespace
        checkArgument(!isWhitespace(path.charAt(path.length() - 1)), "File location cannot end with whitespace: %s", location);
    }
}
