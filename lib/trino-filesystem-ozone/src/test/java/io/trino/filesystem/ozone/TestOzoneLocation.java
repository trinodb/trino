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
package io.trino.filesystem.ozone;

import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOzoneLocation
{
    @Test
    public void testValidUri()
    {
        assertS3Uri("o3://vol1/bucket1/x", "vol1", "bucket1", "x");
        assertS3Uri("o3://vol1/bucket2/xyz/fooBAR", "vol1", "bucket2", "xyz/fooBAR");
        assertS3Uri("o3://vol1/bucket1/xyz/../foo", "vol1", "bucket1", "xyz/../foo");
        assertS3Uri("o3://vol1/bucket2/..", "vol1", "bucket2", "..");
        assertS3Uri("o3://vol2/bucket1/xyz/%41%xx", "vol2", "bucket1", "xyz/%41%xx");
        assertS3Uri("o3://vol2/bucket2///what", "vol2", "bucket2", "//what");
        assertS3Uri("o3://vol2/bucket1///what//", "vol2", "bucket1", "//what//");
    }

    @Test
    public void testInvalidUri()
    {
        // TODO
//        assertThatThrownBy(() -> new OzoneLocation(Location.of("/vol/xyz")))
//                .isInstanceOf(IllegalArgumentException.class)
//                .hasMessage("No scheme for o3 location: /vol/xyz");
//
//        assertThatThrownBy(() -> new OzoneLocation(Location.of("o3://")))
//                .isInstanceOf(IllegalArgumentException.class)
//                .hasMessage("No bucket for S3 location: o3://");
//
//        assertThatThrownBy(() -> new OzoneLocation(Location.of("o3://vol")))
//                .isInstanceOf(IllegalArgumentException.class)
//                .hasMessage("Path missing in file system location: o3://vol");
//
//        assertThatThrownBy(() -> new OzoneLocation(Location.of("o3:///vol")))
//                .isInstanceOf(IllegalArgumentException.class)
//                .hasMessage("No bucket for S3 location: o3:///vol");
//
//        assertThatThrownBy(() -> new OzoneLocation(Location.of("o3://user:pass@vol/xyz")))
//                .isInstanceOf(IllegalArgumentException.class)
//                .hasMessage("S3 location contains user info: o3://user:pass@vol/xyz");
//
//        assertThatThrownBy(() -> new OzoneLocation(Location.of("blah://vol/xyz")))
//                .isInstanceOf(IllegalArgumentException.class)
//                .hasMessage("Wrong scheme for S3 location: blah://vol/xyz");
    }

    private static void assertS3Uri(String uri, String volume, String bucket, String key)
    {
        var location = Location.of(uri);
        var OzoneLocation = new OzoneLocation(location);
        assertThat(OzoneLocation.location()).as("location").isEqualTo(location);
        assertThat(OzoneLocation.volume()).as("volume").isEqualTo(volume);
        assertThat(OzoneLocation.bucket()).as("bucket").isEqualTo(bucket);
        assertThat(OzoneLocation.key()).as("key").isEqualTo(key);
    }
}
