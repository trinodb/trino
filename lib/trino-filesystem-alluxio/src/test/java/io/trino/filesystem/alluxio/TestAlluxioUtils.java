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
package io.trino.filesystem.alluxio;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAlluxioUtils
{
    @Test
    public void testSimplifyPath()
    {
        String path = "test/level0-file0";
        assertThat(path).isEqualTo(AlluxioUtils.simplifyPath(path));
        path = "a/./b/../../c/";
        assertThat("c/").isEqualTo(AlluxioUtils.simplifyPath(path));
    }

    @Test
    public void convertToLocation()
    {
        String mountRoot = "/";
        URIStatus fileStatus = new URIStatus(new FileInfo().setPath("/mnt/test/level0-file0"));
        assertThat(Location.of("alluxio:///mnt/test/level0-file0")).isEqualTo(AlluxioUtils.convertToLocation(fileStatus, mountRoot));
        fileStatus = new URIStatus(new FileInfo().setPath("/mnt/test/level0/level1-file0"));
        assertThat(Location.of("alluxio:///mnt/test/level0/level1-file0")).isEqualTo(AlluxioUtils.convertToLocation(fileStatus, mountRoot));
        fileStatus = new URIStatus(new FileInfo().setPath("/mnt/test2/level0/level1/level2-file0"));
        assertThat(Location.of("alluxio:///mnt/test2/level0/level1/level2-file0")).isEqualTo(AlluxioUtils.convertToLocation(fileStatus, mountRoot));
    }

    @Test
    public void testConvertToAlluxioURI()
    {
        Location location = Location.of("alluxio:///mnt/test/level0-file0");
        String mountRoot = "/";
        assertThat(new AlluxioURI("/mnt/test/level0-file0")).isEqualTo(AlluxioUtils.convertToAlluxioURI(location, mountRoot));
        location = Location.of("alluxio:///mnt/test/level0/level1-file0");
        assertThat(new AlluxioURI("/mnt/test/level0/level1-file0")).isEqualTo(AlluxioUtils.convertToAlluxioURI(location, mountRoot));
        location = Location.of("alluxio:///mnt/test2/level0/level1/level2-file0");
        assertThat(new AlluxioURI("/mnt/test2/level0/level1/level2-file0")).isEqualTo(AlluxioUtils.convertToAlluxioURI(location, mountRoot));
    }
}
