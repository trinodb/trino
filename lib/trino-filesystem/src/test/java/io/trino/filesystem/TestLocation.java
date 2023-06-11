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
import java.util.OptionalInt;

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
        assertLocation("sc heme://user info@ho st/so me/pa th", "sc heme", Optional.of("user info"), "ho st", "so me/pa th");

        // userInfo is optional
        assertLocation("scheme://host/some/path", "scheme", Optional.empty(), "host", "some/path");
        // userInfo can be empty string
        assertLocation("scheme://@host/some/path", "scheme", Optional.of(""), "host", "some/path");

        // host can be empty string
        assertLocation("scheme:///some/path", "scheme", Optional.empty(), "", "some/path");
        // userInfo can be empty string
        assertLocation("scheme://user@/some/path", "scheme", Optional.of("user"), "", "some/path");
        // host and userInfo can both be empty
        assertLocation("scheme://@/some/path", "scheme", Optional.of(""), "", "some/path");

        // port is allowed
        assertLocation("hdfs://hadoop:9000/some/path", "hdfs", "hadoop", 9000, "some/path");

        // path can contain anything
        assertLocation("scheme://host/..", "scheme", Optional.empty(), "host", "..");

        assertLocation("scheme://host/path/../../other", "scheme", Optional.empty(), "host", "path/../../other");

        assertLocation("scheme://host/path/%41%illegal", "scheme", Optional.empty(), "host", "path/%41%illegal");

        assertLocation("scheme://host///path", "scheme", Optional.empty(), "host", "//path");

        assertLocation("scheme://host///path//", "scheme", Optional.empty(), "host", "//path//");

        // the path can be empty
        assertLocation("scheme://host", "scheme", Optional.empty(), "host", "");
        assertLocation("scheme://", "scheme", Optional.empty(), "", "");
        assertLocation("scheme://host/", "scheme", Optional.empty(), "host", "");
        assertLocation("scheme:///", "scheme", Optional.empty(), "", "");

        // the path can be just a slash (if you really want)
        assertLocation("scheme://host//", "scheme", Optional.empty(), "host", "/");
        assertLocation("scheme:////", "scheme", Optional.empty(), "", "/");

        // the location can be just a path
        assertLocation("/", "");
        assertLocation("/abc", "abc");
        assertLocation("/abc/xyz", "abc/xyz");
        assertLocation("/foo://host:port/path", "foo://host:port/path");

        // special handling for Locations without hostnames
        assertLocation("file:/", "file", "");
        assertLocation("file:/hello.txt", "file", "hello.txt");
        assertLocation("file:/some/path", "file", "some/path");
        assertLocation("file:/some@what/path", "file", "some@what/path");
        assertLocation("hdfs:/a/hadoop/path.csv", "hdfs", "a/hadoop/path.csv");

        // invalid locations
        assertThatThrownBy(() -> Location.of(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> Location.of(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("location is empty");
        assertThatThrownBy(() -> Location.of("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("location is blank");
        assertThatThrownBy(() -> Location.of("x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scheme");
        assertThatThrownBy(() -> Location.of("scheme://host:invalid/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port");

        // fragment is not allowed
        assertThatThrownBy(() -> Location.of("scheme://userInfo@host/some/path#fragement"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Fragment");
        assertThatThrownBy(() -> Location.of("scheme://userInfo@ho#st/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Fragment");
        assertThatThrownBy(() -> Location.of("scheme://user#Info@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Fragment");
        assertThatThrownBy(() -> Location.of("sc#heme://userInfo@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Fragment");

        // query component is not allowed
        assertThatThrownBy(() -> Location.of("scheme://userInfo@host/some/path?fragement"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query");
        assertThatThrownBy(() -> Location.of("scheme://userInfo@ho?st/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query");
        assertThatThrownBy(() -> Location.of("scheme://user?Info@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query");
        assertThatThrownBy(() -> Location.of("sc?heme://userInfo@host/some/path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("query");
    }

    private static void assertLocation(String locationString, String scheme, Optional<String> userInfo, String host, String path)
    {
        Location location = Location.of(locationString);
        Optional<String> expectedHost = host.isEmpty() ? Optional.empty() : Optional.of(host);
        assertLocation(location, locationString, Optional.of(scheme), userInfo, expectedHost, OptionalInt.empty(), path);
    }

    private static void assertLocation(String locationString, String scheme, String path)
    {
        Location location = Location.of(locationString);
        assertLocation(location, locationString, Optional.of(scheme), Optional.empty(), Optional.empty(), OptionalInt.empty(), path);
    }

    private static void assertLocation(String locationString, String scheme, String host, int port, String path)
    {
        Location location = Location.of(locationString);
        assertLocation(location, locationString, Optional.of(scheme), Optional.empty(), Optional.of(host), OptionalInt.of(port), path);
    }

    private static void assertLocation(String locationString, String path)
    {
        Location location = Location.of(locationString);
        assertLocation(location, locationString, Optional.empty(), Optional.empty(), Optional.empty(), OptionalInt.empty(), path);
    }

    private static void assertLocation(Location actual, Location expected)
    {
        assertLocation(actual, expected.toString(), expected.scheme(), expected.userInfo(), expected.host(), expected.port(), expected.path());
    }

    private static void assertLocation(Location location, String locationString, Optional<String> scheme, Optional<String> userInfo, Optional<String> host, OptionalInt port, String path)
    {
        assertThat(location.toString()).isEqualTo(locationString);
        assertThat(location.scheme()).isEqualTo(scheme);
        assertThat(location.userInfo()).isEqualTo(userInfo);
        assertThat(location.host()).isEqualTo(host);
        assertThat(location.port()).isEqualTo(port);
        assertThat(location.path()).isEqualTo(path);

        assertThat(location).isEqualTo(location);
        assertThat(location).isEqualTo(Location.of(locationString));
        assertThat(location.hashCode()).isEqualTo(location.hashCode());
        assertThat(location.hashCode()).isEqualTo(Location.of(locationString).hashCode());

        assertThat(location.toString()).isEqualTo(locationString);
    }

    @Test
    void testVerifyFileLocation()
    {
        Location.of("scheme://userInfo@host/name").verifyValidFileLocation();
        Location.of("scheme://userInfo@host/path/name").verifyValidFileLocation();

        Location.of("/name").verifyValidFileLocation();
        Location.of("/path/name").verifyValidFileLocation();

        assertInvalidFileLocation("scheme://userInfo@host", "File location must contain a path");
        assertInvalidFileLocation("scheme://userInfo@host/", "File location must contain a path");
        assertInvalidFileLocation("scheme://userInfo@host/name/", "File location cannot end with '/'");
        assertInvalidFileLocation("scheme://userInfo@host/name ", "File location cannot end with whitespace");

        assertInvalidFileLocation("/", "File location must contain a path");
        assertInvalidFileLocation("/name/", "File location cannot end with '/'");
        assertInvalidFileLocation("/name ", "File location cannot end with whitespace");
    }

    private static void assertInvalidFileLocation(String locationString, String expectedErrorMessage)
    {
        Location location = Location.of(locationString);
        assertThatThrownBy(location::verifyValidFileLocation)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(locationString)
                .hasMessageContaining(expectedErrorMessage);
        assertThatThrownBy(location::fileName)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(locationString)
                .hasMessageContaining(expectedErrorMessage);
        assertThatThrownBy(location::parentDirectory)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(locationString)
                .hasMessageContaining(expectedErrorMessage);
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
    void testParentDirectory()
    {
        assertParentDirectory("scheme://userInfo@host/path/name", Location.of("scheme://userInfo@host/path"));
        assertParentDirectory("scheme://userInfo@host:1234/name", Location.of("scheme://userInfo@host:1234"));

        assertParentDirectory("scheme://userInfo@host/path//name", Location.of("scheme://userInfo@host/path/"));
        assertParentDirectory("scheme://userInfo@host/path///name", Location.of("scheme://userInfo@host/path//"));
        assertParentDirectory("scheme://userInfo@host/path:/name", Location.of("scheme://userInfo@host/path:"));

        assertParentDirectory("/path/name", Location.of("/path"));
        assertParentDirectory("/name", Location.of("/"));

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

    @Test
    void testAppendPath()
    {
        assertAppendPath("scheme://userInfo@host", "name", Location.of("scheme://userInfo@host/name"));
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
        assertAppendPath("/", "/name", Location.of("/name"));
        assertAppendPath("/path", "name", Location.of("/path/name"));
        assertAppendPath("/path", "/name", Location.of("/path/name"));
        assertAppendPath("/path/", "/name", Location.of("/path/name"));
        assertAppendPath("/path/", "name", Location.of("/path/name"));
    }

    private static void assertAppendPath(String locationString, String newPathElement, Location expected)
    {
        Location location = Location.of(locationString).appendPath(newPathElement);
        assertLocation(location, expected);
    }

    @Test
    void testAppendSuffix()
    {
        assertAppendSuffix("scheme://userInfo@host", ".ext", Location.of("scheme://userInfo@host/.ext"));
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
