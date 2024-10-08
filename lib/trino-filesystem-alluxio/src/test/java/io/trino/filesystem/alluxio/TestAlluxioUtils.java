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
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import static io.trino.filesystem.alluxio.AlluxioUtils.convertToAlluxioURI;
import static io.trino.filesystem.alluxio.AlluxioUtils.convertToLocation;
import static io.trino.filesystem.alluxio.AlluxioUtils.simplifyPath;
import static org.assertj.core.api.Assertions.assertThat;

final class TestAlluxioUtils
{
    @Test
    void testSimplifyPath()
    {
        assertThat(simplifyPath("test/level0-file0")).isEqualTo("test/level0-file0");
        assertThat(simplifyPath("a/./b/../../c/")).isEqualTo("c/");
    }

    @Test
    void testConvertToLocation()
    {
        assertThat(convertToLocation("/mnt/test/level0-file0", "/"))
                .isEqualTo(Location.of("alluxio:///mnt/test/level0-file0"));
        assertThat(convertToLocation("/mnt/test/level0/level1-file0", "/"))
                .isEqualTo(Location.of("alluxio:///mnt/test/level0/level1-file0"));
        assertThat(convertToLocation("/mnt/test2/level0/level1/level2-file0", "/"))
                .isEqualTo(Location.of("alluxio:///mnt/test2/level0/level1/level2-file0"));
    }

    @Test
    void testConvertToAlluxioURI()
    {
        assertThat(convertToAlluxioURI(Location.of("alluxio:///mnt/test/level0-file0"), "/"))
                .isEqualTo(new AlluxioURI("/mnt/test/level0-file0"));
        assertThat(convertToAlluxioURI(Location.of("alluxio:///mnt/test/level0/level1-file0"), "/"))
                .isEqualTo(new AlluxioURI("/mnt/test/level0/level1-file0"));
        assertThat(convertToAlluxioURI(Location.of("alluxio:///mnt/test2/level0/level1/level2-file0"), "/"))
                .isEqualTo(new AlluxioURI("/mnt/test2/level0/level1/level2-file0"));
    }
}
