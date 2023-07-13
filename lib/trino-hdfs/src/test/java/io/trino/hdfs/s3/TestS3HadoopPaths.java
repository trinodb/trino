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
package io.trino.hdfs.s3;

import io.trino.filesystem.Location;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.net.URI;

import static io.trino.filesystem.hdfs.HadoopPaths.hadoopPath;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TestS3HadoopPaths
{
    @Test
    public void testNonS3Path()
    {
        assertThat(hadoopPath(Location.of("gcs://test/abc//xyz")))
                .isEqualTo(new Path("gcs://test/abc/xyz"));
    }

    @Test
    public void testS3NormalPath()
    {
        assertThat(hadoopPath(Location.of("s3://test/abc/xyz.csv")))
                .isEqualTo(new Path("s3://test/abc/xyz.csv"))
                .extracting(TrinoS3FileSystem::keyFromPath)
                .isEqualTo("abc/xyz.csv");
    }

    @Test
    public void testS3NormalPathWithInvalidUriEscape()
    {
        assertThat(hadoopPath(Location.of("s3://test/abc%xyz")))
                .isEqualTo(new Path("s3://test/abc%xyz"))
                .extracting(TrinoS3FileSystem::keyFromPath)
                .isEqualTo("abc%xyz");
    }

    @Test
    public void testS3NonCanonicalPath()
    {
        assertThat(hadoopPath(Location.of("s3://test/abc//xyz.csv")))
                .isEqualTo(new Path(URI.create("s3://test/abc/xyz.csv#abc//xyz.csv")))
                .hasToString("s3://test/abc/xyz.csv#abc//xyz.csv")
                .extracting(TrinoS3FileSystem::keyFromPath)
                .isEqualTo("abc//xyz.csv");
    }

    @Test
    public void testS3NonCanonicalPathWithInvalidUriEscape()
    {
        assertThat(hadoopPath(Location.of("s3://test/abc%xyz//test")))
                .isEqualTo(new Path(URI.create("s3://test/abc%25xyz/test#abc%25xyz//test")))
                .hasToString("s3://test/abc%xyz/test#abc%xyz//test")
                .extracting(TrinoS3FileSystem::keyFromPath)
                .isEqualTo("abc%xyz//test");
    }
}
