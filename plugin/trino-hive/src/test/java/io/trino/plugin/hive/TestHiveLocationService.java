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
import io.trino.plugin.hive.LocationService.WriteInfo;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY;
import static io.trino.plugin.hive.LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;

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

    @Test
    public void testGetTableWriteInfoOverwriteFailDirectNew()
    {
        assertTrinoExceptionThrownBy(() -> assertThat(locationHandle(DIRECT_TO_TARGET_NEW_DIRECTORY, "/target", "/target"), true))
                .hasMessage("Overwriting unpartitioned table not supported when writing directly to target directory");
    }

    @Test
    public void testGetTableWriteInfoOverwriteFailDirectExisting()
    {
        assertTrinoExceptionThrownBy(() -> assertThat(locationHandle(DIRECT_TO_TARGET_EXISTING_DIRECTORY, "/target", "/target"), true))
                .hasMessage("Overwriting unpartitioned table not supported when writing directly to target directory");
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
            LocationService service = new HiveLocationService(HDFS_FILE_SYSTEM_FACTORY, new HiveConfig());
            this.actual = service.getTableWriteInfo(locationHandle, overwrite);
        }

        public void producesWriteInfo(WriteInfo expected)
        {
            Assertions.assertThat(actual.writePath()).isEqualTo(expected.writePath());
            Assertions.assertThat(actual.targetPath()).isEqualTo(expected.targetPath());
            Assertions.assertThat(actual.writeMode()).isEqualTo(expected.writeMode());
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
