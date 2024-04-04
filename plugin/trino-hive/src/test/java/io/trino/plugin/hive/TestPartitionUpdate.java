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
import io.airlift.json.JsonCodec;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.PartitionUpdate.UpdateMode;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitionUpdate
{
    private static final JsonCodec<PartitionUpdate> CODEC = jsonCodec(PartitionUpdate.class);

    @Test
    public void testRoundTrip()
    {
        PartitionUpdate expected = new PartitionUpdate(
                "test",
                UpdateMode.APPEND,
                "/writePath",
                "/targetPath",
                ImmutableList.of("file1", "file3"),
                123,
                456,
                789);

        PartitionUpdate actual = CODEC.fromJson(CODEC.toJson(expected));

        assertThat(actual.getName()).isEqualTo("test");
        assertThat(actual.getUpdateMode()).isEqualTo(UpdateMode.APPEND);
        assertThat(actual.getWritePath()).isEqualTo(Location.of("/writePath"));
        assertThat(actual.getTargetPath()).isEqualTo(Location.of("/targetPath"));
        assertThat(actual.getFileNames()).isEqualTo(ImmutableList.of("file1", "file3"));
        assertThat(actual.getRowCount()).isEqualTo(123);
        assertThat(actual.getInMemoryDataSizeInBytes()).isEqualTo(456);
        assertThat(actual.getOnDiskDataSizeInBytes()).isEqualTo(789);
    }
}
