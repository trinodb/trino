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

import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLocation
{
    @Test
    void testParse()
    {
        assertLocation("scheme://userInfo@host/some/path", "scheme", Optional.of("userInfo"), "host", "some/path");
        // case is preserved
        assertLocation("SCHEME://USER_INFO@HOST/SOME/PATH", "SCHEME", Optional.of("USER_INFO"), "HOST", "SOME/PATH");
        // whitespace is allowed
        assertLocation("sc heme://user info@ho st/so me/pa th", Optional.of("sc heme"), Optional.of("user info"), Optional.of("ho st"), OptionalInt.empty(), "so me/pa th", Set.of("Illegal character in scheme name at index 2: sc heme://user info@ho st/so me/pa th"));

        // userInfo is optional
        assertLocation("scheme://host/some/path", "scheme", Optional.empty(), "host", "some/path");
        // userInfo can be empty string
        assertLocation("scheme://@host/some/path", "scheme", Optional.of(""), "host", "some/path");

        // host can be empty string
        assertLocation("scheme:///some/path", "scheme", Optional.empty(), "", "some/path");
        // host can be empty string when userInfo is present
        assertLocation("scheme://user@/some/path", Optional.of("scheme"), Optional.of("user"), Optional.empty(), OptionalInt.empty(), "some/path", Set.of("userInfo compared with URI: expected [Optional.empty], was [Optional[user]]"));
        // userInfo cannot contain slashes
        assertLocation("scheme://host:1234/some/path//@here:444/there", Optional.of("scheme"), Optional.empty(), Optional.of("host"), OptionalInt.of(1234), "some/path//@here:444/there");
        // host and userInfo can both be empty
        assertLocation("scheme://@/some/path", Optional.of("scheme"), Optional.of(""), Optional.empty(), OptionalInt.empty(), "some/path", Set.of("userInfo compared with URI: expected [Optional.empty], was [Optional[]]"));
        // port or userInfo can be given even if host is not (note: this documents current state, but does not imply the intent to support such locations)
        assertLocation("scheme://:1/some/path", Optional.of("scheme"), Optional.empty(), Optional.empty(), OptionalInt.of(1), "some/path", Set.of("port compared with URI: expected [OptionalInt.empty], was [OptionalInt[1]]"));
        assertLocation("scheme://@:1/some/path", Optional.of("scheme"), Optional.of(""), Optional.empty(), OptionalInt.of(1), "some/path", Set.of(
                "userInfo compared with URI: expected [Optional.empty], was [Optional[]]",
                "port compared with URI: expected [OptionalInt.empty], was [OptionalInt[1]]"));
        assertLocation("scheme://@/", Optional.of("scheme"), Optional.of(""), Optional.empty(), OptionalInt.empty(), "", Set.of("userInfo compared with URI: expected [Optional.empty], was [Optional[]]"));
        assertLocation("scheme://@:1/", Optional.of("scheme"), Optional.of(""), Optional.empty(), OptionalInt.of(1), "", Set.of(
                "userInfo compared with URI: expected [Optional.empty], was [Optional[]]",
                "port compared with URI: expected [OptionalInt.empty], was [OptionalInt[1]]"));
        assertLocation("scheme://:1/", Optional.of("scheme"), Optional.empty(), Optional.empty(), OptionalInt.of(1), "", Set.of("port compared with URI: expected [OptionalInt.empty], was [OptionalInt[1]]"));
        assertLocation("scheme://@//", Optional.of("scheme"), Optional.of(""), Optional.empty(), OptionalInt.empty(), "/", Set.of("userInfo compared with URI: expected [Optional.empty], was [Optional[]]"));
        assertLocation("scheme://@:1//", Optional.of("scheme"), Optional.of(""), Optional.empty(), OptionalInt.of(1), "/", Set.of(
                "userInfo compared with URI: expected [Optional.empty], was [Optional[]]",
                "port compared with URI: expected [OptionalInt.empty], was [OptionalInt[1]]"));
        assertLocation("scheme://:1//", Optional.of("scheme"), Optional.empty(), Optional.empty(), OptionalInt.of(1), "/", Set.of("port compared with URI: expected [OptionalInt.empty], was [OptionalInt[1]]"));

        // port is allowed
        assertLocation("hdfs://hadoop:9000/some/path", "hdfs", "hadoop", 9000, "some/path");

        // path can contain anything
        assertLocation("scheme://host/..", "scheme", Optional.empty(), "host", "..");

        assertLocation("scheme://host/path/../../other", "scheme", Optional.empty(), "host", "path/../../other");

        assertLocation("scheme://host/path/%41%illegal", Optional.of("scheme"), Optional.empty(), Optional.of("host"), OptionalInt.empty(), "path/%41%illegal", Set.of("Malformed escape pair at index 22: scheme://host/path/%41%illegal"));

        assertLocation("scheme://host///path", "scheme", Optional.empty(), "host", "//path");

        assertLocation("scheme://host///path//", "scheme", Optional.empty(), "host", "//path//");

        // the path can be empty
        assertLocation("scheme://", Optional.of("scheme"), Optional.empty(), Optional.empty(), OptionalInt.empty(), "", Set.of("Expected authority at index 9: scheme://"));
        assertLocation("scheme://host/", "scheme", Optional.empty(), "host", "");
        assertLocation("scheme:///", "scheme", Optional.empty(), "", "");

        // the path can be just a slash (if you really want)
        assertLocation("scheme://host//", "scheme", Optional.empty(), "host", "/");
        assertLocation("scheme:////", "scheme", Optional.empty(), "", "/");

        // the location can be just a path
        assertLocation("/", "");
        assertLocation("//", "/", Set.of("Expected authority at index 2: //"));
        assertLocation("///", "//", Set.of("'/path' compared with URI: expected [/], was [///]"));
        assertLocation("/abc", "abc");
        assertLocation("//abc", "/abc", Set.of("host compared with URI: expected [Optional[abc]], was [Optional.empty]", "'/path' compared with URI: expected [], was [//abc]"));
        assertLocation("///abc", "//abc", Set.of("'/path' compared with URI: expected [/abc], was [///abc]"));
        assertLocation("/abc/xyz", "abc/xyz");
        assertLocation("/foo://host:port/path", "foo://host:port/path");

        // special handling for Locations without hostnames
        assertLocation("file:/", "file", "");
        assertLocation("file://", Optional.of("file"), Optional.empty(), Optional.empty(), OptionalInt.empty(), "", Set.of("Expected authority at index 7: file://"));
        assertLocation("file:///", "file", "");
        assertLocation("file:////", "file", "/");
        assertLocation("file://///", "file", "//");
        assertLocation("file:/hello.txt", "file", "hello.txt");
        assertLocation("file:/some/path", "file", "some/path");
        assertLocation("file:/some@what/path", "file", "some@what/path");
        assertLocation("hdfs:/a/hadoop/path.csv", "hdfs", "a/hadoop/path.csv");
        assertLocation("file:///tmp/staging/dir/some-user@example.com", "file", "tmp/staging/dir/some-user@example.com");

        // invalid locations
        assertThatThrownBy(() -> Location.of(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> Location.of(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("location is empty");
        assertThatThrownBy(() -> Location.of("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("location is blank");
        assertThatThrownBy(() -> Location.of("x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No scheme for file system location: x");
        assertThatThrownBy(() -> Location.of("dev/null"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No scheme for file system location: dev/null");
        assertThatThrownBy(() -> Location.of("scheme://host:invalid/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid port in file system location: scheme://host:invalid/path");
        assertThatThrownBy(() -> Location.of("scheme://:"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid port in file system location: scheme://:");
        assertThatThrownBy(() -> Location.of("scheme://:/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid port in file system location: scheme://:/");
        assertThatThrownBy(() -> Location.of("scheme://@:/"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid port in file system location: scheme://@:/");

        // no path
        assertThatThrownBy(() -> Location.of("scheme://host"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: scheme://host");

        assertThatThrownBy(() -> Location.of("scheme://userInfo@host"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: scheme://userInfo@host");

        assertThatThrownBy(() -> Location.of("scheme://userInfo@host:1234"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: scheme://userInfo@host:1234");

        // no path and empty host name
        assertThatThrownBy(() -> Location.of("scheme://@"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: scheme://@");

        assertThatThrownBy(() -> Location.of("scheme://@:1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: scheme://@:1");

        assertThatThrownBy(() -> Location.of("scheme://:1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: scheme://:1");

        assertThatThrownBy(() -> Location.of("scheme://:"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid port in file system location: scheme://:");

        // fragment is not allowed
        assertThatThrownBy(() -> Location.of("scheme://userInfo@host/some/path#fragement"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Fragment is not allowed in a file system location: scheme://userInfo@host/some/path#fragement");
        assertThatThrownBy(() -> Location.of("scheme://userInfo@ho#st/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Fragment is not allowed in a file system location: scheme://userInfo@ho#st/some/path");
        assertThatThrownBy(() -> Location.of("scheme://user#Info@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Fragment is not allowed in a file system location: scheme://user#Info@host/some/path");
        assertThatThrownBy(() -> Location.of("sc#heme://userInfo@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Fragment is not allowed in a file system location: sc#heme://userInfo@host/some/path");

        // query component is not allowed
        assertThatThrownBy(() -> Location.of("scheme://userInfo@host/some/path?fragement"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URI query component is not allowed in a file system location: scheme://userInfo@host/some/path?fragement");
        assertThatThrownBy(() -> Location.of("scheme://userInfo@ho?st/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URI query component is not allowed in a file system location: scheme://userInfo@ho?st/some/path");
        assertThatThrownBy(() -> Location.of("scheme://user?Info@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URI query component is not allowed in a file system location: scheme://user?Info@host/some/path");
        assertThatThrownBy(() -> Location.of("sc?heme://userInfo@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("URI query component is not allowed in a file system location: sc?heme://userInfo@host/some/path");
    }

    private static void assertLocation(String locationString, String scheme, Optional<String> userInfo, String host, String path)
    {
        Optional<String> expectedHost = host.isEmpty() ? Optional.empty() : Optional.of(host);
        assertLocation(locationString, Optional.of(scheme), userInfo, expectedHost, OptionalInt.empty(), path, Set.of());
    }

    private static void assertLocation(String locationString, String scheme, String path)
    {
        assertLocation(locationString, Optional.of(scheme), Optional.empty(), Optional.empty(), OptionalInt.empty(), path, Set.of());
    }

    private static void assertLocation(String locationString, String scheme, String host, int port, String path)
    {
        assertLocation(locationString, Optional.of(scheme), Optional.empty(), Optional.of(host), OptionalInt.of(port), path, Set.of());
    }

    private static void assertLocation(String locationString, String path)
    {
        assertLocation(locationString, path, Set.of());
    }

    private static void assertLocation(String locationString, String path, Set<String> uriIncompatibilities)
    {
        assertLocation(locationString, Optional.empty(), Optional.empty(), Optional.empty(), OptionalInt.empty(), path, uriIncompatibilities);
    }

    private static void assertLocation(Location actual, Location expected)
    {
        assertLocation(actual, expected.toString(), expected.scheme(), expected.userInfo(), expected.host(), expected.port(), expected.path(), true, Set.of("skipped"));
    }

    private static void assertLocation(String locationString, Optional<String> scheme, Optional<String> userInfo, Optional<String> host, OptionalInt port, String path)
    {
        assertLocation(locationString, scheme, userInfo, host, port, path, Set.of());
    }

    private static void assertLocation(
            String locationString,
            Optional<String> scheme,
            Optional<String> userInfo,
            Optional<String> host,
            OptionalInt port,
            String path,
            Set<String> uriIncompatibilities)
    {
        Location location = Location.of(locationString);
        assertLocation(location, locationString, scheme, userInfo, host, port, path, false, uriIncompatibilities);
    }

    private static void assertLocation(
            Location location,
            String locationString,
            Optional<String> scheme,
            Optional<String> userInfo,
            Optional<String> host,
            OptionalInt port,
            String path,
            boolean skipUriTesting,
            Set<String> uriIncompatibilities)
    {
        assertThat(location.toString()).isEqualTo(locationString);
        assertThat(location.scheme()).isEqualTo(scheme);
        assertThat(location.userInfo()).isEqualTo(userInfo);
        assertThat(location.host()).isEqualTo(host);
        assertThat(location.port()).isEqualTo(port);
        assertThat(location.path()).isEqualTo(path);

        if (!skipUriTesting) {
            int observedIncompatibilities = 0;
            URI uri = null;
            try {
                uri = new URI(locationString);
            }
            catch (URISyntaxException e) {
                assertThat(uriIncompatibilities).contains(e.getMessage());
                observedIncompatibilities++;
            }
            // Locations are not URIs but they follow URI structure
            if (uri != null) {
                observedIncompatibilities += assertEqualsOrVerifyDeviation(Optional.ofNullable(uri.getScheme()), location.scheme(), "scheme compared with URI", uriIncompatibilities);
                observedIncompatibilities += assertEqualsOrVerifyDeviation(Optional.ofNullable(uri.getUserInfo()), location.userInfo(), "userInfo compared with URI", uriIncompatibilities);
                observedIncompatibilities += assertEqualsOrVerifyDeviation(Optional.ofNullable(uri.getHost()), location.host(), "host compared with URI", uriIncompatibilities);
                observedIncompatibilities += assertEqualsOrVerifyDeviation(uri.getPort() == -1 ? OptionalInt.empty() : OptionalInt.of(uri.getPort()), location.port(), "port compared with URI", uriIncompatibilities);
                // For some reason, URI.getPath returns paths starting with "/", while Location does not.
                observedIncompatibilities += assertEqualsOrVerifyDeviation(uri.getPath(), "/" + location.path(), "'/path' compared with URI", uriIncompatibilities);
            }
            assertThat(uriIncompatibilities).hasSize(observedIncompatibilities);
        }
        else {
            assertThat(uriIncompatibilities).isEqualTo(Set.of("skipped"));
        }

        assertThat(location).isEqualTo(location);
        assertThat(location).isEqualTo(Location.of(locationString));
        assertThat(location.hashCode()).isEqualTo(location.hashCode());
        assertThat(location.hashCode()).isEqualTo(Location.of(locationString).hashCode());

        assertThat(location.toString()).isEqualTo(locationString);
    }

    private static <T> int assertEqualsOrVerifyDeviation(T expected, T actual, String message, Set<String> expectedDifferences)
    {
        if (Objects.equals(expected, actual)) {
            return 0;
        }
        String key = "%s: expected [%s], was [%s]".formatted(message, expected, actual);
        assertThat(expectedDifferences).contains(key);
        return 1;
    }

    @Test
    void testVerifyFileLocation()
    {
        Location.of("scheme://userInfo@host/name").verifyValidFileLocation();
        Location.of("scheme://userInfo@host/path/name").verifyValidFileLocation();
        Location.of("scheme://userInfo@host/name ").verifyValidFileLocation();

        Location.of("/name").verifyValidFileLocation();
        Location.of("/path/name").verifyValidFileLocation();
        Location.of("/path/name ").verifyValidFileLocation();
        Location.of("/name ").verifyValidFileLocation();

        assertInvalidFileLocation("scheme://userInfo@host/", "File location must contain a path: scheme://userInfo@host/");
        assertInvalidFileLocation("scheme://userInfo@host/name/", "File location cannot end with '/': scheme://userInfo@host/name/");

        assertInvalidFileLocation("/", "File location must contain a path: /");
        assertInvalidFileLocation("/name/", "File location cannot end with '/': /name/");
    }

    private static void assertInvalidFileLocation(String locationString, String expectedErrorMessage)
    {
        Location location = Location.of(locationString);
        assertThatThrownBy(location::verifyValidFileLocation)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(locationString)
                .hasMessage(expectedErrorMessage);
        assertThatThrownBy(location::fileName)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(locationString)
                .hasMessage(expectedErrorMessage);
        assertThatThrownBy(location::parentDirectory)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(locationString)
                .hasMessage(expectedErrorMessage);
    }

    @Test
    void testFileName()
    {
        assertFileName("scheme://userInfo@host/path/name", "name");
        assertFileName("scheme://userInfo@host/name", "name");

        assertFileName("/path/name", "name");
        assertFileName("/name", "name");

        // all valid file locations must have a path
        // invalid file locations are tested in testVerifyFileLocation
    }

    private static void assertFileName(String locationString, String fileName)
    {
        // fileName method only works with valid file locations
        Location location = Location.of(locationString);
        location.verifyValidFileLocation();
        assertThat(location.fileName()).isEqualTo(fileName);
    }

    @Test
    void testSibling()
    {
        assertSiblingFailure("/", "sibling", IllegalStateException.class, "File location must contain a path: /");
        assertSiblingFailure("//", "sibling", IllegalStateException.class, "File location must contain a path: /");
        assertSiblingFailure("file:/", "sibling", IllegalStateException.class, "File location must contain a path: file:/");
        assertSiblingFailure("file://", "sibling", IllegalStateException.class, "File location must contain a path: file://");
        assertSiblingFailure("file:///", "sibling", IllegalStateException.class, "File location must contain a path: file:///");
        assertSiblingFailure("s3://bucket/", "sibling", IllegalStateException.class, "File location must contain a path: s3://bucket/");
        assertSiblingFailure("scheme://userInfo@host/path/", "sibling", IllegalStateException.class, "File location cannot end with '/'");

        assertSiblingFailure("scheme://userInfo@host/path/filename", null, NullPointerException.class, "name is null");
        assertSiblingFailure("scheme://userInfo@host/path/filename", "", IllegalArgumentException.class, "name is empty");

        assertSibling("scheme://userInfo@host/path/name", "sibling", "scheme://userInfo@host/path/sibling");
        assertSibling("scheme://userInfo@host/path//name", "sibling", "scheme://userInfo@host/path//sibling");
        assertSibling("scheme://userInfo@host/path///name", "sibling", "scheme://userInfo@host/path///sibling");
        assertSibling("scheme://userInfo@host/level1/level2/name", "sibling", "scheme://userInfo@host/level1/level2/sibling");
        assertSibling("scheme://userInfo@host/level1//level2/name", "sibling", "scheme://userInfo@host/level1//level2/sibling");

        assertSibling("file:/path/name", "sibling", "file:/path/sibling");
        assertSibling("s3://bucket/directory/filename with spaces", "sibling", "s3://bucket/directory/sibling");
        assertSibling("/path/name", "sibling", "/path/sibling");
        assertSibling("/name", "sibling", "/sibling");
    }

    private static void assertSiblingFailure(String locationString, String siblingName, Class<?> exceptionClass, String exceptionMessage)
    {
        assertThatThrownBy(() -> Location.of(locationString).sibling(siblingName))
                .isInstanceOf(exceptionClass)
                .hasMessageContaining(exceptionMessage);
    }

    private static void assertSibling(String locationString, String siblingName, String expectedLocationString)
    {
        // fileName method only works with valid file locations
        Location location = Location.of(locationString);
        location.verifyValidFileLocation();
        Location siblingLocation = location.sibling(siblingName);

        assertLocation(siblingLocation, Location.of(expectedLocationString));
    }

    @Test
    void testParentDirectory()
    {
        assertParentDirectoryFailure("scheme:/", "File location must contain a path: scheme:/");
        assertParentDirectoryFailure("scheme://", "File location must contain a path: scheme://");
        assertParentDirectoryFailure("scheme:///", "File location must contain a path: scheme:///");

        assertParentDirectoryFailure("scheme://host/", "File location must contain a path: scheme://host/");
        assertParentDirectoryFailure("scheme://userInfo@host/", "File location must contain a path: scheme://userInfo@host/");
        assertParentDirectoryFailure("scheme://userInfo@host:1234/", "File location must contain a path: scheme://userInfo@host:1234/");

        assertParentDirectoryFailure("scheme://host//", "File location must contain a path: scheme://host//");
        assertParentDirectoryFailure("scheme://userInfo@host//", "File location must contain a path: scheme://userInfo@host//");
        assertParentDirectoryFailure("scheme://userInfo@host:1234//", "File location must contain a path: scheme://userInfo@host:1234//");

        assertParentDirectory("scheme://userInfo@host/path/name", Location.of("scheme://userInfo@host/path"));
        assertParentDirectory("scheme://userInfo@host:1234/name", Location.of("scheme://userInfo@host:1234/"));

        assertParentDirectory("scheme://userInfo@host/path//name", Location.of("scheme://userInfo@host/path/"));
        assertParentDirectory("scheme://userInfo@host/path///name", Location.of("scheme://userInfo@host/path//"));
        assertParentDirectory("scheme://userInfo@host/path:/name", Location.of("scheme://userInfo@host/path:"));

        assertParentDirectoryFailure("/", "File location must contain a path: /");
        assertParentDirectoryFailure("//", "File location must contain a path: //");
        assertParentDirectory("/path/name", Location.of("/path"));
        assertParentDirectory("/name", Location.of("/"));

        assertParentDirectoryFailure("/path/name/", "File location cannot end with '/': /path/name/");
        assertParentDirectoryFailure("/name/", "File location cannot end with '/': /name/");

        assertParentDirectory("/path//name", Location.of("/path/"));
        assertParentDirectory("/path///name", Location.of("/path//"));
        assertParentDirectory("/path:/name", Location.of("/path:"));

        // all valid file locations must have a parent directory
        // invalid file locations are tested in testVerifyFileLocation
    }

    private static void assertParentDirectory(String locationString, Location parentLocation)
    {
        // fileName method only works with valid file locations
        Location location = Location.of(locationString);
        location.verifyValidFileLocation();
        Location parentDirectory = location.parentDirectory();

        assertLocation(parentDirectory, parentLocation);
    }

    private static void assertParentDirectoryFailure(String locationString, @Language("RegExp") String expectedMessagePattern)
    {
        assertThatThrownBy(Location.of(locationString)::parentDirectory)
                .hasMessageMatching(expectedMessagePattern);
    }

    @Test
    void testAppendPath()
    {
        assertAppendPath("scheme://userInfo@host/", "name", Location.of("scheme://userInfo@host/name"));

        assertAppendPath("scheme://userInfo@host:1234/path", "name", Location.of("scheme://userInfo@host:1234/path/name"));
        assertAppendPath("scheme://userInfo@host/path/", "name", Location.of("scheme://userInfo@host/path/name"));

        assertAppendPath("scheme://userInfo@host/path//", "name", Location.of("scheme://userInfo@host/path//name"));
        assertAppendPath("scheme://userInfo@host/path:", "name", Location.of("scheme://userInfo@host/path:/name"));

        assertAppendPath("scheme://", "name", Location.of("scheme:///name"));
        assertAppendPath("scheme:///", "name", Location.of("scheme:///name"));

        assertAppendPath("scheme:///path", "name", Location.of("scheme:///path/name"));
        assertAppendPath("scheme:///path/", "name", Location.of("scheme:///path/name"));

        assertAppendPath("/", "name", Location.of("/name"));
        assertAppendPath("/path", "name", Location.of("/path/name"));

        assertAppendPath("/tmp", "username@example.com", Location.of("/tmp/username@example.com"));
        assertAppendPath("file:///tmp", "username@example.com", Location.of("file:///tmp/username@example.com"));
    }

    private static void assertAppendPath(String locationString, String newPathElement, Location expected)
    {
        Location location = Location.of(locationString).appendPath(newPathElement);
        assertLocation(location, expected);
    }

    @Test
    void testAppendSuffix()
    {
        assertAppendSuffix("scheme://userInfo@host/", ".ext", Location.of("scheme://userInfo@host/.ext"));

        assertAppendSuffix("scheme://userInfo@host:1234/path", ".ext", Location.of("scheme://userInfo@host:1234/path.ext"));
        assertAppendSuffix("scheme://userInfo@host/path/", ".ext", Location.of("scheme://userInfo@host/path/.ext"));

        assertAppendSuffix("scheme://userInfo@host/path//", ".ext", Location.of("scheme://userInfo@host/path//.ext"));
        assertAppendSuffix("scheme://userInfo@host/path:", ".ext", Location.of("scheme://userInfo@host/path:.ext"));

        assertAppendSuffix("scheme://", ".ext", Location.of("scheme:///.ext"));
        assertAppendSuffix("scheme:///", ".ext", Location.of("scheme:///.ext"));

        assertAppendSuffix("scheme:///path", ".ext", Location.of("scheme:///path.ext"));
        assertAppendSuffix("scheme:///path/", ".ext", Location.of("scheme:///path/.ext"));

        assertAppendSuffix("scheme:///path", "/foo", Location.of("scheme:///path/foo"));
        assertAppendSuffix("scheme:///path/", "/foo", Location.of("scheme:///path//foo"));

        assertAppendSuffix("/", ".ext", Location.of("/.ext"));
        assertAppendSuffix("/path", ".ext", Location.of("/path.ext"));
    }

    private static void assertAppendSuffix(String locationString, String suffix, Location expected)
    {
        Location location = Location.of(locationString).appendSuffix(suffix);
        assertLocation(location, expected);
    }
}
