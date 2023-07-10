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

import com.google.common.base.Splitter;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

/**
 * Location of a file or directory in a blob or hierarchical file system.
 * The location uses the URI like format {@code scheme://[userInfo@]host[:port][/path]}, but does not
 * follow the format rules of a URI or URL which support escapes and other special characters.
 * <p>
 * Alternatively, a location can be specified as {@code /path} for usage with legacy HDFS installations,
 * or as {@code file:/path} for local file systems as returned by {@link File#toURI()}.
 * <p>
 * The API of this class is very limited, so blob storage locations can be used as well. Specifically,
 * methods are provided to get the name of a file location, get the parent of a location, append a path
 * to a location, and parse a location. This allows for the operations needed for analysing data in an
 * object store where you need to create subdirectories, and get peers of a file. Specifically, walking
 * up a path is discouraged as some blob locations have invalid inner path parts.
 */
public final class Location
{
    private static final Splitter SCHEME_SPLITTER = Splitter.on(":").limit(2);
    private static final Splitter USER_INFO_SPLITTER = Splitter.on('@').limit(2);
    private static final Splitter AUTHORITY_SPLITTER = Splitter.on('/').limit(2);
    private static final Splitter HOST_AND_PORT_SPLITTER = Splitter.on(':').limit(2);

    private final String location;
    private final Optional<String> scheme;
    private final Optional<String> userInfo;
    private final Optional<String> host;
    private final OptionalInt port;
    private final String path;

    public static Location of(String location)
    {
        requireNonNull(location, "location is null");
        checkArgument(!location.isEmpty(), "location is empty");
        checkArgument(!location.isBlank(), "location is blank");

        checkArgument(location.indexOf('#') < 0, "Fragment is not allowed in a file system location: %s", location);
        checkArgument(location.indexOf('?') < 0, "URI query component is not allowed in a file system location: %s", location);

        // legacy HDFS location that is just a path
        if (location.startsWith("/")) {
            return new Location(location, Optional.empty(), Optional.empty(), Optional.empty(), OptionalInt.empty(), location.substring(1));
        }

        List<String> schemeSplit = SCHEME_SPLITTER.splitToList(location);
        checkArgument(schemeSplit.size() == 2, "No scheme for file system location: %s", location);
        String scheme = schemeSplit.get(0);

        String afterScheme = schemeSplit.get(1);
        if (afterScheme.startsWith("//")) {
            // Locations with an authority must begin with a double slash
            afterScheme = afterScheme.substring(2);

            List<String> authoritySplit = AUTHORITY_SPLITTER.splitToList(afterScheme);
            List<String> userInfoSplit = USER_INFO_SPLITTER.splitToList(authoritySplit.get(0));
            Optional<String> userInfo = userInfoSplit.size() == 2 ? Optional.of(userInfoSplit.get(0)) : Optional.empty();
            List<String> hostAndPortSplit = HOST_AND_PORT_SPLITTER.splitToList(getLast(userInfoSplit));

            Optional<String> host = Optional.of(hostAndPortSplit.get(0)).filter(not(String::isEmpty));

            OptionalInt port = OptionalInt.empty();
            if (hostAndPortSplit.size() == 2) {
                try {
                    port = OptionalInt.of(parseInt(hostAndPortSplit.get(1)));
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port in file system location: " + location, e);
                }
            }

            checkArgument((userInfo.isEmpty() && host.isEmpty() && port.isEmpty()) || authoritySplit.size() == 2, "Path missing in file system location: %s", location);
            String path = (authoritySplit.size() == 2) ? authoritySplit.get(1) : "";

            return new Location(location, Optional.of(scheme), userInfo, host, port, path);
        }

        checkArgument(afterScheme.startsWith("/"), "Path must begin with a '/' when no authority is present");
        return new Location(location, Optional.of(scheme), Optional.empty(), Optional.empty(), OptionalInt.empty(), afterScheme.substring(1));
    }

    private Location(String location, Optional<String> scheme, Optional<String> userInfo, Optional<String> host, OptionalInt port, String path)
    {
        this.location = requireNonNull(location, "location is null");
        this.scheme = requireNonNull(scheme, "scheme is null");
        this.userInfo = requireNonNull(userInfo, "userInfo is null");
        this.host = requireNonNull(host, "host is null");
        this.port = requireNonNull(port, "port is null");
        this.path = requireNonNull(path, "path is null");
        checkArgument(scheme.isEmpty() || !scheme.get().isEmpty(), "scheme value is empty");
        checkArgument(host.isEmpty() || !host.get().isEmpty(), "host value is empty");
    }

    private Location withPath(String location, String path)
    {
        return new Location(location, scheme, userInfo, host, port, path);
    }

    /**
     * Returns the scheme of the location, if present.
     * If the scheme is present, the value will not be an empty string.
     * Legacy HDFS paths do not have a scheme.
     */
    public Optional<String> scheme()
    {
        return scheme;
    }

    /**
     * Returns the user info of the location, if present.
     * The user info will be present if the location authority contains an at sign,
     * but the value may be an empty string.
     */
    public Optional<String> userInfo()
    {
        return userInfo;
    }

    /**
     * Returns the host of the location, if present.
     * If the host is present, the value will not be an empty string.
     */
    public Optional<String> host()
    {
        return host;
    }

    public OptionalInt port()
    {
        return port;
    }

    /**
     * Returns the path of the location. The path will not start with a slash, and might be empty.
     */
    public String path()
    {
        return path;
    }

    /**
     * Returns the file name of the location.
     * The location must be a valid file location.
     * The file name is all characters after the last slash in the path.
     *
     * @throws IllegalStateException if the location is not a valid file location
     */
    public String fileName()
    {
        verifyValidFileLocation();
        return path.substring(path.lastIndexOf('/') + 1);
    }

    /**
     * Returns a new location with the same parent directory as the current location,
     * but with the filename corresponding to the specified name.
     * The location must be a valid file location.
     */
    public Location sibling(String name)
    {
        requireNonNull(name, "name is null");
        checkArgument(!name.isEmpty(), "name is empty");
        verifyValidFileLocation();

        return this.withPath(location.substring(0, location.lastIndexOf('/') + 1) + name, path.substring(0, path.lastIndexOf('/') + 1) + name);
    }

    /**
     * Creates a new location with all characters removed after the last slash in the path.
     * This should only be used once, as recursive calls for blob paths may lead to incorrect results.
     *
     * @throws IllegalStateException if the location is not a valid file location
     */
    public Location parentDirectory()
    {
        // todo should this only be allowed for file locations?
        verifyValidFileLocation();
        checkState(!path.isEmpty() && !path.equals("/"), "root location does not have parent: %s", location);

        int lastIndexOfSlash = path.lastIndexOf('/');
        if (lastIndexOfSlash < 0) {
            String newLocation = location.substring(0, location.length() - path.length() - 1);
            newLocation += "/";
            return withPath(newLocation, "");
        }

        String newPath = path.substring(0, lastIndexOfSlash);
        String newLocation = location.substring(0, location.length() - (path.length() - newPath.length()));
        return withPath(newLocation, newPath);
    }

    /**
     * Creates a new location by appending the given path element to the current path.
     * A slash will be added between the current path and the new path element if needed.
     *
     * @throws IllegalArgumentException if the new path element is empty or starts with a slash
     */
    public Location appendPath(String newPathElement)
    {
        checkArgument(!newPathElement.isEmpty(), "newPathElement is empty");
        checkArgument(!newPathElement.startsWith("/"), "newPathElement starts with a slash: %s", newPathElement);

        if (path.isEmpty()) {
            return appendToEmptyPath(newPathElement);
        }

        if (!path.endsWith("/")) {
            newPathElement = "/" + newPathElement;
        }
        return withPath(location + newPathElement, path + newPathElement);
    }

    Location removeOneTrailingSlash()
    {
        if (path.endsWith("/")) {
            return withPath(location.substring(0, location.length() - 1), path.substring(0, path.length() - 1));
        }
        if (path.equals("") && location.endsWith("/")) {
            return withPath(location.substring(0, location.length() - 1), "");
        }
        return this;
    }

    /**
     * Creates a new location by appending the given suffix to the current path.
     * Typical usage for this method is to append a file extension to a file name,
     * but it may be used to append anything, including a slash.
     * <p>
     * Use {@link #appendPath(String)} instead of this method to append a path element.
     */
    public Location appendSuffix(String suffix)
    {
        if (path.isEmpty()) {
            return appendToEmptyPath(suffix);
        }

        return withPath(location + suffix, path + suffix);
    }

    private Location appendToEmptyPath(String value)
    {
        checkState(path.isEmpty());

        // empty path may or may not have a location that ends with a slash
        boolean needSlash = !location.endsWith("/");

        // slash is needed for locations with no host or user info that did not have a path
        if (scheme.isPresent() && host.isEmpty() && userInfo.isEmpty() && !location.endsWith(":///")) {
            needSlash = true;
        }

        return withPath(location + (needSlash ? "/" : "") + value, value);
    }

    /**
     * Verifies the location is valid for a file reference.  Specifically, the path must not be empty and must not end with a slash.
     *
     * @throws IllegalStateException if the location is not a valid file location
     */
    public void verifyValidFileLocation()
    {
        // TODO: should this be IOException?
        // file path must not be empty
        checkState(!path.isEmpty() && !path.equals("/"), "File location must contain a path: %s", location);
        // file path cannot end with a slash
        checkState(!path.endsWith("/"), "File location cannot end with '/': %s", location);
    }

    @Override
    public boolean equals(Object o)
    {
        return (o instanceof Location that) && location.equals(that.location);
    }

    @Override
    public int hashCode()
    {
        return location.hashCode();
    }

    /**
     * Return the original location string.
     */
    @Override
    public String toString()
    {
        return location;
    }
}
