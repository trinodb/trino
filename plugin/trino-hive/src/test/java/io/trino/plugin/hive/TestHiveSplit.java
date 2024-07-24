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
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.Location;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.plugin.hive.HiveColumnHandle.ColumnType;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveSplit
{
    @Test
    public void testJsonRoundTrip()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(new TestingTypeManager())));
        JsonCodec<HiveSplit> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(HiveSplit.class);

        Map<String, String> schema = ImmutableMap.<String, String>builder()
                .put("foo", "bar")
                .put("bar", "baz")
                .buildOrThrow();

        ImmutableList<HivePartitionKey> partitionKeys = ImmutableList.of(new HivePartitionKey("a", "apple"), new HivePartitionKey("b", "42"));
        ImmutableList<HostAddress> addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 44), HostAddress.fromParts("127.0.0.1", 45));

        AcidInfo.Builder acidInfoBuilder = AcidInfo.builder(Location.of("file:///data/fullacid"));
        acidInfoBuilder.addDeleteDelta(Location.of("file:///data/fullacid/delete_delta_0000004_0000004_0000"));
        acidInfoBuilder.addDeleteDelta(Location.of("file:///data/fullacid/delete_delta_0000007_0000007_0000"));
        AcidInfo acidInfo = acidInfoBuilder.build().get();

        HiveSplit expected = new HiveSplit(
                "partitionId",
                "path",
                42,
                87,
                88,
                Instant.now().toEpochMilli(),
                schema,
                partitionKeys,
                addresses,
                OptionalInt.empty(),
                OptionalInt.empty(),
                true,
                ImmutableMap.of(1, new HiveTypeName("string")),
                Optional.of(new HiveSplit.BucketConversion(
                        BUCKETING_V1,
                        32,
                        16,
                        ImmutableList.of(createBaseColumn("col", 5, HIVE_LONG, BIGINT, ColumnType.REGULAR, Optional.of("comment"))))),
                Optional.empty(),
                Optional.of(acidInfo),
                SplitWeight.fromProportion(2.0)); // some non-standard value

        String json = codec.toJson(expected);
        HiveSplit actual = codec.fromJson(json);

        assertThat(actual.getPartitionName()).isEqualTo(expected.getPartitionName());
        assertThat(actual.getPath()).isEqualTo(expected.getPath());
        assertThat(actual.getStart()).isEqualTo(expected.getStart());
        assertThat(actual.getLength()).isEqualTo(expected.getLength());
        assertThat(actual.getEstimatedFileSize()).isEqualTo(expected.getEstimatedFileSize());
        assertThat(actual.getSchema()).isEqualTo(expected.getSchema());
        assertThat(actual.getPartitionKeys()).isEqualTo(expected.getPartitionKeys());
        assertThat(actual.getHiveColumnCoercions()).isEqualTo(expected.getHiveColumnCoercions());
        assertThat(actual.getBucketConversion()).isEqualTo(expected.getBucketConversion());
        assertThat(actual.isForceLocalScheduling()).isEqualTo(expected.isForceLocalScheduling());
        assertThat(actual.getAcidInfo().get()).isEqualTo(expected.getAcidInfo().get());
        assertThat(actual.getSplitWeight()).isEqualTo(expected.getSplitWeight());
    }
}
