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
package io.trino.plugin.hive;

import io.trino.filesystem.Location;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.net.URI;

import static io.trino.plugin.hive.HiveWriterFactory.computeNonTransactionalBucketedFilename;
import static io.trino.plugin.hive.HiveWriterFactory.computeTransactionalBucketedFilename;
import static io.trino.plugin.hive.HiveWriterFactory.setSchemeToFileIfAbsent;
import static org.apache.hadoop.hive.ql.exec.Utilities.getBucketIdFromFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestHiveWriterFactory
{
    @Test
    public void testComputeBucketedFileName()
    {
        String name = computeNonTransactionalBucketedFilename("20180102_030405_00641_x1y2z", 1234);
        assertThat(name).matches("001234_0_.*_20180102_030405_00641_x1y2z");
        assertEquals(getBucketIdFromFile(name), 1234);

        name = computeTransactionalBucketedFilename(1234);
        assertEquals(name, "001234_0");
        assertEquals(getBucketIdFromFile(name), 1234);
    }

    @Test
    public void testSetsSchemeToFile()
    {
        String pathWithoutScheme = "/simple/file/path";
        String result = setSchemeToFileIfAbsent(Location.of(pathWithoutScheme)).toString();
        assertThat(result).isEqualTo("file:///simple/file/path");
        URI resultUri = new Path(result).toUri();
        assertThat(resultUri.getScheme()).isEqualTo("file");
        assertThat(resultUri.getPath()).isEqualTo("/simple/file/path");

        String pathWithScheme = "s3://simple/file/path";
        result = setSchemeToFileIfAbsent(Location.of(pathWithScheme)).toString();
        assertThat(result).isEqualTo(pathWithScheme);
        resultUri = new Path(result).toUri();
        assertThat(resultUri.getScheme()).isEqualTo("s3");
        assertThat(resultUri.getPath()).isEqualTo("/file/path");

        String pathWithEmptySpaces = "/simple/file 1/path";
        result = setSchemeToFileIfAbsent(Location.of(pathWithEmptySpaces)).toString();
        assertThat(result).isEqualTo("file:///simple/file 1/path");
        resultUri = new Path(result).toUri();
        assertThat(resultUri.getScheme()).isEqualTo("file");
        assertThat(resultUri.getPath()).isEqualTo("/simple/file 1/path");

        String pathWithEmptySpacesAndScheme = "s3://simple/file 1/path";
        result = setSchemeToFileIfAbsent(Location.of(pathWithEmptySpacesAndScheme)).toString();
        assertThat(result).isEqualTo(pathWithEmptySpacesAndScheme);
        resultUri = new Path(result).toUri();
        assertThat(resultUri.getScheme()).isEqualTo("s3");
        assertThat(resultUri.getPath()).isEqualTo("/file 1/path");

        String pathWithAtSign = "/tmp/user@example.com";
        result = setSchemeToFileIfAbsent(Location.of(pathWithAtSign)).toString();
        assertThat(result).isEqualTo("file:///tmp/user@example.com");
        resultUri = new Path(result).toUri();
        assertThat(resultUri.getScheme()).isEqualTo("file");
        assertThat(resultUri.getPath()).isEqualTo("/tmp/user@example.com");
    }
}
