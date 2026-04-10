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

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitionData
{
    @Test
    public void testToJsonWithNullReturnsEmptyObject()
    {
        assertThat(PartitionData.toJson(null)).isEqualTo("{}");
    }

    @Test
    public void testToJsonWithEmptyPartitionData()
    {
        PartitionData empty = new PartitionData(new Object[0]);
        String json = PartitionData.toJson(empty);
        assertThat(json).isEqualTo("{\"partitionValues\":[]}");
    }

    @Test
    public void testToJsonRoundTrip()
    {
        PartitionData original = new PartitionData(new Object[] {42, "hello"});
        String json = PartitionData.toJson(original);
        assertThat(json).contains("\"partitionValues\"");

        PartitionData restored = PartitionData.fromJson(json,
                new org.apache.iceberg.types.Type[] {Types.IntegerType.get(), Types.StringType.get()});
        assertThat(restored.size()).isEqualTo(2);
        assertThat(restored.get(0, Integer.class)).isEqualTo(42);
        assertThat(restored.get(1, String.class)).isEqualTo("hello");
    }

    @Test
    public void testFromJsonWithNullReturnsNull()
    {
        assertThat(PartitionData.fromJson(null, new org.apache.iceberg.types.Type[0])).isNull();
    }
}
