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

import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.hive.HiveWriterFactory.computeBucketedFileName;
import static org.apache.hadoop.hive.ql.exec.Utilities.getBucketIdFromFile;
import static org.testng.Assert.assertEquals;

public class TestHiveWriterFactory
{
    @Test
    public void testComputeBucketedFileName()
    {
        String name = computeBucketedFileName(Optional.of("20180102_030405_00641_x1y2z"), 1234);
        assertEquals(name, "001234_0_20180102_030405_00641_x1y2z");
        assertEquals(getBucketIdFromFile(name), 1234);

        name = computeBucketedFileName(Optional.empty(), 1234);
        assertEquals(name, "001234_0");
        assertEquals(getBucketIdFromFile(name), 1234);
    }
}
