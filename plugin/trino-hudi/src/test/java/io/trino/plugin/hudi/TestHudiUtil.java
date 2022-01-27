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
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hive.HiveStylePartitionValueExtractor;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.hive.SlashEncodedHourPartitionValueExtractor;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormat;
import static io.trino.plugin.hudi.HudiUtil.buildPartitionValues;
import static io.trino.plugin.hudi.HudiUtil.isHudiParquetInputFormat;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
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

        assertTrue(isHudiParquetInputFormat(getInputFormat(new Configuration(false), schema, false)));
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

    @Test
    public void testInferPartitionValueExtractor()
    {
        assertEquals(HudiUtil.inferPartitionValueExtractor(
                        "2022/01/05", Collections.singletonList("2022-01-05")).getClass().getName(),
                SlashEncodedDayPartitionValueExtractor.class.getName());
        assertEquals(HudiUtil.inferPartitionValueExtractor(
                        "2022/01/05/19", Collections.singletonList("2022-01-05-19")).getClass().getName(),
                SlashEncodedHourPartitionValueExtractor.class.getName());
        assertEquals(HudiUtil.inferPartitionValueExtractor(
                        "country=united_states",
                        Collections.singletonList("united_states")).getClass().getName(),
                HiveStylePartitionValueExtractor.class.getName());
        assertEquals(HudiUtil.inferPartitionValueExtractor(
                        "country=united_states/city=san_francisco",
                        ImmutableList.of("united_states", "san_francisco")).getClass().getName(),
                MultiPartKeysValueExtractor.class.getName());
        assertThatThrownBy(() -> HudiUtil.inferPartitionValueExtractor(
                "randompartitionpath", Collections.singletonList("")));
    }

    private static void assertToPartitionValues(String partitionName, List<String> expected)
    {
        List<String> actual = buildPartitionValues(partitionName);
        assertEquals(actual, expected);
    }
}
