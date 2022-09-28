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
package io.trino.plugin.iceberg;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructLikeWrapper;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStructLikeWrapperWithFieldIdToIndex
{
    @Test
    public void testStructLikeWrapperWithFieldIdToIndexEquals()
    {
        StructType firstStructType = StructType.of(
                NestedField.optional(1001, "level", StringType.get()),
                NestedField.optional(1002, "event_time_hour", IntegerType.get()));
        StructType secondStructType = StructType.of(
                NestedField.optional(1000, "event_time_day", StringType.get()),
                NestedField.optional(1001, "level", IntegerType.get()));
        PartitionData firstPartitionData = PartitionData.fromJson("{\"partitionValues\":[\"ERROR\",\"449245\"]}", new Type[] {StringType.get(), IntegerType.get()});
        PartitionData secondPartitionData = PartitionData.fromJson("{\"partitionValues\":[\"449245\",\"ERROR\"]}", new Type[] {IntegerType.get(), StringType.get()});
        PartitionTable.StructLikeWrapperWithFieldIdToIndex first = new PartitionTable.StructLikeWrapperWithFieldIdToIndex(StructLikeWrapper.forType(firstStructType).set(firstPartitionData), firstStructType);
        PartitionTable.StructLikeWrapperWithFieldIdToIndex second = new PartitionTable.StructLikeWrapperWithFieldIdToIndex(StructLikeWrapper.forType(secondStructType).set(secondPartitionData), secondStructType);
        assertThat(first).isNotEqualTo(second);
    }
}
