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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.LocationService.WriteInfo;
import io.trino.plugin.hive.TestBackgroundHiveSplitLoader.TestingHdfsEnvironment;
import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static org.testng.Assert.assertEquals;

public class TestHiveLocationService
{
    @Test
    public void testGetTableWriteInfoAppend()
    {
        assertThat(locationHandle(STAGE_AND_MOVE_TO_TARGET_DIRECTORY), false)
                .producesWriteInfo(writeInfo(
                        "/target",
                        "/write",
                        STAGE_AND_MOVE_TO_TARGET_DIRECTORY));

        assertThat(locationHandle(DIRECT_TO_TARGET_EXISTING_DIRECTORY, "/target", "/target"), false)
                .producesWriteInfo(writeInfo(
                        "/target",
                        "/target",
                        DIRECT_TO_TARGET_EXISTING_DIRECTORY));

        assertThat(locationHandle(DIRECT_TO_TARGET_NEW_DIRECTORY, "/target", "/target"), false)
                .producesWriteInfo(writeInfo(
                        "/target",
                        "/target",
                        DIRECT_TO_TARGET_NEW_DIRECTORY));
    }

    @Test
    public void testGetTableWriteInfoOverwriteSuccess()
    {
        assertThat(locationHandle(STAGE_AND_MOVE_TO_TARGET_DIRECTORY), true)
                .producesWriteInfo(writeInfo("/target", "/write", STAGE_AND_MOVE_TO_TARGET_DIRECTORY));
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = "Overwriting unpartitioned table not supported when writing directly to target directory")
    public void testGetTableWriteInfoOverwriteFailDirectNew()
    {
        assertThat(locationHandle(DIRECT_TO_TARGET_NEW_DIRECTORY, "/target", "/target"), true);
    }

    @Test(expectedExceptions = TrinoException.class, expectedExceptionsMessageRegExp = "Overwriting unpartitioned table not supported when writing directly to target directory")
    public void testGetTableWriteInfoOverwriteFailDirectExisting()
    {
        assertThat(locationHandle(DIRECT_TO_TARGET_EXISTING_DIRECTORY, "/target", "/target"), true);
    }

    private static Assertion assertThat(LocationHandle locationHandle, boolean overwrite)
    {
        return new Assertion(locationHandle, overwrite);
    }

    public static class Assertion
    {
        private final WriteInfo actual;

        public Assertion(LocationHandle locationHandle, boolean overwrite)
        {
            HdfsEnvironment hdfsEnvironment = new TestingHdfsEnvironment(ImmutableList.of());
            LocationService service = new HiveLocationService(hdfsEnvironment, new HiveConfig());
            this.actual = service.getTableWriteInfo(locationHandle, overwrite);
        }

        public void producesWriteInfo(WriteInfo expected)
        {
            assertEquals(actual.writePath(), expected.writePath());
            assertEquals(actual.targetPath(), expected.targetPath());
            assertEquals(actual.writeMode(), expected.writeMode());
        }
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode)
    {
        return locationHandle(writeMode, "/target", "/write");
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode, String targetPath, String writePath)
    {
        return new LocationHandle(Location.of(targetPath), Location.of(writePath), writeMode);
    }

    private static WriteInfo writeInfo(String targetPath, String writePath, LocationHandle.WriteMode writeMode)
    {
        return new WriteInfo(Location.of(targetPath), Location.of(writePath), writeMode);
    }
}
