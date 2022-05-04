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
package io.trino.plugin.hidden.partitioning;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.testng.Assert.assertEquals;

public class TestHiddenPartitioningPartitionSpecParser
{
    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "val", Types.StringType.get()),
            optional(3, "event_time", Types.TimestampType.withoutZone()),
            optional(4, "event_date", Types.TimestampType.withoutZone()));

    public static final String DAY_SPEC = "{\n" +
            "  \"fields\" : [ {\n" +
            "    \"name\" : \"event_date\",\n" +
            "    \"transform\" : \"day\",\n" +
            "    \"source-name\" : \"event_time\"\n" +
            "  } ]\n" +
            "}";

    public static final String TRUNCATE_SPEC = "{\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"source-name\": \"epoch_timestamp\",\n" +
            "      \"transform\": \"truncate[86400]\",\n" +
            "      \"name\": \"event_date\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Test
    public void testFromJson()
            throws Exception
    {
        HiddenPartitioningPartitionSpec hiddenPartitionSpec = HiddenPartitioningSpecParser.fromJson(DAY_SPEC);
        assertEquals(1, hiddenPartitionSpec.getFields().size());
        HiddenPartitioningPartitionSpec.PartitionField partitionField = hiddenPartitionSpec.getFields().get(0);
        assertEquals("event_date", partitionField.getName());
        assertEquals("day", partitionField.getTransform());
        assertEquals("event_time", partitionField.getSourceName());
    }

    @Test
    public void testToIcebergPartitionSpec()
            throws Exception
    {
        HiddenPartitioningPartitionSpec hiddenPartitionSpec = HiddenPartitioningSpecParser.fromJson(DAY_SPEC);
        PartitionSpec partitionSpec = HiddenPartitioningSpecParser.toIcebergPartitionSpec(hiddenPartitionSpec, SCHEMA);
        assertEquals(1, partitionSpec.getFieldsBySourceId(3).size());
    }

    @Test
    public void testValidateSpecJson()
    {
        HiddenPartitioningSpecParser.validateSpecJson(DAY_SPEC);
    }

    @Test
    public void testValidateTruncateSpecJson()
    {
        HiddenPartitioningSpecParser.validateSpecJson(TRUNCATE_SPEC);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "partition spec name is null")
    public void testValidateSpecJsonMissingName()
    {
        String badSpec = "{\n" +
                "  \"fields\" : [ {\n" +
                "    \"transform\" : \"day\",\n" +
                "    \"source-name\" : \"event_time\"\n" +
                "  } ]\n" +
                "}";
        HiddenPartitioningSpecParser.validateSpecJson(badSpec);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "partition spec transform is null")
    public void testValidateSpecJsonMissingTransform()
    {
        String badSpec = "{\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"event_date\",\n" +
                "    \"source-name\" : \"event_time\"\n" +
                "  } ]\n" +
                "}";
        HiddenPartitioningSpecParser.validateSpecJson(badSpec);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "partition spec source name is null")
    public void testValidateSpecJsonMissingSourceName()
    {
        String badSpec = "{\n" +
                "  \"fields\" : [ {\n" +
                "    \"name\" : \"event_date\",\n" +
                "    \"transform\" : \"day\"\n" +
                "  } ]\n" +
                "}";
        HiddenPartitioningSpecParser.validateSpecJson(badSpec);
    }
}
