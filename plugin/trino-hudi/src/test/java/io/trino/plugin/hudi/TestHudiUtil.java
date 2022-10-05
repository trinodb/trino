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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Properties;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormat;
import static io.trino.plugin.hudi.HudiUtil.isHudiParquetInputFormat;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHudiUtil
{
    @Test
    public void testIsHudiParquetInputFormat()
    {
        Properties schema = new Properties();
        schema.setProperty(FILE_INPUT_FORMAT, HoodieParquetInputFormat.class.getName());
        schema.setProperty(SERIALIZATION_LIB, PARQUET.getSerde());

        assertTrue(isHudiParquetInputFormat(getInputFormat(newEmptyConfiguration(), schema, false)));
    }

    @Test
    public void testBuildPartitionValues()
    {
        assertToPartitionValues("partitionColumn1=01/01/2020", ImmutableList.of("01/01/2020"));
        assertToPartitionValues("partitionColumn1=01/01/2020/partitioncolumn2=abc", ImmutableList.of("01/01/2020", "abc"));
        assertToPartitionValues("ds=2015-12-30/event_type=QueryCompletion", ImmutableList.of("2015-12-30", "QueryCompletion"));
        assertToPartitionValues("ds=2015-12-30", ImmutableList.of("2015-12-30"));
        assertToPartitionValues("a=1", ImmutableList.of("1"));
        assertToPartitionValues("a=1/b=2/c=3", ImmutableList.of("1", "2", "3"));
        assertToPartitionValues("pk=!@%23$%25%5E&%2A()%2F%3D", ImmutableList.of("!@#$%^&*()/="));
        assertToPartitionValues("pk=__HIVE_DEFAULT_PARTITION__", ImmutableList.of("__HIVE_DEFAULT_PARTITION__"));
    }

    private static void assertToPartitionValues(String partitionName, List<String> expected)
    {
        List<String> actual = buildPartitionValues(partitionName);
        assertEquals(actual, expected);
    }

    private static List<String> buildPartitionValues(String partitionNames)
    {
        ImmutableList.Builder<String> values = ImmutableList.builder();
        String[] parts = partitionNames.split("=");
        if (parts.length == 1) {
            values.add(unescapePathName(partitionNames));
            return values.build();
        }
        if (parts.length == 2) {
            values.add(unescapePathName(parts[1]));
            return values.build();
        }
        for (int i = 1; i < parts.length; i++) {
            String val = parts[i];
            int j = val.lastIndexOf('/');
            if (j == -1) {
                values.add(unescapePathName(val));
            }
            else {
                values.add(unescapePathName(val.substring(0, j)));
            }
        }
        return values.build();
    }
}
