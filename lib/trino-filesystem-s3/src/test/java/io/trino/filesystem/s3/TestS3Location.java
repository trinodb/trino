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
package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestS3Location
{
    @Test
    public void testValidUri()
    {
        assertS3Uri("s3://abc/", "abc", "");
        assertS3Uri("s3://abc/x", "abc", "x");
        assertS3Uri("s3://abc/xyz/fooBAR", "abc", "xyz/fooBAR");
        assertS3Uri("s3://abc/xyz/../foo", "abc", "xyz/../foo");
        assertS3Uri("s3://abc/..", "abc", "..");
        assertS3Uri("s3://abc/xyz/%41%xx", "abc", "xyz/%41%xx");
        assertS3Uri("s3://abc///what", "abc", "//what");
        assertS3Uri("s3://abc///what//", "abc", "//what//");
        assertS3Uri("s3a://hello/what/xxx", "hello", "what/xxx");
    }

    @Test
    public void testInvalidUri()
    {
        assertThatThrownBy(() -> new S3Location(Location.of("/abc/xyz")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No scheme for S3 location: /abc/xyz");

        assertThatThrownBy(() -> new S3Location(Location.of("s3://")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No bucket for S3 location: s3://");

        assertThatThrownBy(() -> new S3Location(Location.of("s3://abc")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Path missing in file system location: s3://abc");

        assertThatThrownBy(() -> new S3Location(Location.of("s3:///abc")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No bucket for S3 location: s3:///abc");

        assertThatThrownBy(() -> new S3Location(Location.of("s3://user:pass@abc/xyz")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("S3 location contains user info: s3://user:pass@abc/xyz");

        assertThatThrownBy(() -> new S3Location(Location.of("blah://abc/xyz")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Wrong scheme for S3 location: blah://abc/xyz");
    }

    private static void assertS3Uri(String uri, String bucket, String key)
    {
        var location = Location.of(uri);
        var s3Location = new S3Location(location);
        assertThat(s3Location.location()).as("location").isEqualTo(location);
        assertThat(s3Location.bucket()).as("bucket").isEqualTo(bucket);
        assertThat(s3Location.key()).as("key").isEqualTo(key);
    }
}
