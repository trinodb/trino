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
package io.prestosql.plugin.hive;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class TestHiveLocationService
{
    @Test
    public void testGetTableWriteInfo_append()
    {
        assertThat(locationHandle(LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY), false)
                .produceWriteInfoAs(new LocationService.WriteInfo(
                        new Path("hdfs://dir001/target"),
                        new Path("hdfs://dir001/write"),
                        LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY));

        assertThat(locationHandle(
                LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY,
                "hdfs://dir001/target",
                "hdfs://dir001/target"),
                false)
                .produceWriteInfoAs(new LocationService.WriteInfo(
                        new Path("hdfs://dir001/target"),
                        new Path("hdfs://dir001/target"),
                        LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY));

        assertThat(locationHandle(
                LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY,
                "hdfs://dir001/target",
                "hdfs://dir001/target"),
                false)
                .produceWriteInfoAs(new LocationService.WriteInfo(
                        new Path("hdfs://dir001/target"),
                        new Path("hdfs://dir001/target"),
                        LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY));
    }

    @Test
    public void testGetTableWriteInfo_overwriteSuccess()
    {
        assertThat(locationHandle(LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY), true)
                .produceWriteInfoAs(new LocationService.WriteInfo(
                        new Path("hdfs://dir001/target"),
                        new Path("hdfs://dir001/write"),
                        LocationHandle.WriteMode.STAGE_AND_MOVE_TO_TARGET_DIRECTORY));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetTableWriteInfo_overwriteFail_1()
    {
        assertThat(locationHandle(LocationHandle.WriteMode.DIRECT_TO_TARGET_NEW_DIRECTORY,
                "hdfs://dir001/target", "hdfs://dir001/target"), true);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testGetTableWriteInfo_overwriteFail_2()
    {
        assertThat(locationHandle(LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY,
                "hdfs://dir001/target", "hdfs://dir001/target"), true);
    }

    private static Assertion assertThat(LocationHandle locationHandle, boolean overwrite)
    {
        return new Assertion(locationHandle, overwrite);
    }

    public static class Assertion
    {
        private LocationService.WriteInfo actual;

        public Assertion(LocationHandle locationHandle, boolean overwrite)
        {
            HdfsEnvironment hdfsEnvironment = mock(HdfsEnvironment.class);
            HiveLocationService service = new HiveLocationService(hdfsEnvironment);
            this.actual = service.getTableWriteInfo(locationHandle, overwrite);
        }

        public Assertion produceWriteInfoAs(LocationService.WriteInfo expected)
        {
            assertEquals(actual.getWritePath(), expected.getWritePath());
            assertEquals(actual.getTargetPath(), expected.getTargetPath());
            assertEquals(actual.getWriteMode(), expected.getWriteMode());

            return this;
        }
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode)
    {
        return locationHandle(writeMode, "hdfs://dir001/target",
                "hdfs://dir001/write");
    }

    private static LocationHandle locationHandle(LocationHandle.WriteMode writeMode,
            String targetPath, String writePath)
    {
        return new LocationHandle(
                new Path(targetPath),
                new Path(writePath),
                true,
                writeMode);
    }
}
