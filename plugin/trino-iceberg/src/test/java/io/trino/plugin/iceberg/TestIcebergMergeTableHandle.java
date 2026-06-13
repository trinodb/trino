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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.iceberg.delete.DeleteFile;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Round-trip tests for the coordinator-plans-once expansions of {@link IcebergMergeTableHandle}.
 * The handle carries both {@code preExistingDeletesByDataFile} and the frozen
 * {@code baseTableProperties} so merge-writer tasks need neither to re-scan manifests nor to
 * re-read {@code metadata.json}. These tests lock in Jackson round-trip fidelity for the new
 * fields and verify the merge-on-read shape where both maps are empty.
 */
class TestIcebergMergeTableHandle
{
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    private static final PartitionSpec UNPARTITIONED = PartitionSpec.unpartitioned();

    @Test
    void roundTripCarriesPreExistingDeletesAndBaseTableProperties()
    {
        Map<String, String> baseProperties = ImmutableMap.of(
                "write.format.default", "parquet",
                "write.parquet.compression-codec", "zstd",
                "write.metadata.metrics.default", "truncate(16)");

        IcebergMergeTableHandle expected = new IcebergMergeTableHandle(
                baseTableHandle(),
                baseWritableTableHandle(),
                RowLevelOperationMode.COPY_ON_WRITE,
                baseProperties,
                ImmutableMap.of(
                        "s3://bucket/data/file-a.parquet", ImmutableList.of(samplePositionDeleteFile("s3://bucket/deletes/pos-a.parquet")),
                        "s3://bucket/data/file-b.parquet", ImmutableList.of(
                                samplePositionDeleteFile("s3://bucket/deletes/pos-b.parquet"),
                                sampleDeletionVector("s3://bucket/deletes/dv-b.puffin", "s3://bucket/data/file-b.parquet"))));

        IcebergMergeTableHandle actual = codec().fromJson(codec().toJson(expected));

        assertThat(actual.getRowLevelOperationMode()).isEqualTo(expected.getRowLevelOperationMode());
        assertThat(actual.getBaseTableProperties()).isEqualTo(baseProperties);
        assertThat(actual.getPreExistingDeletesByDataFile()).isEqualTo(expected.getPreExistingDeletesByDataFile());
    }

    @Test
    void roundTripMergeOnReadHandleHasEmptyCopyOnWriteState()
    {
        // In MERGE_ON_READ mode the coordinator ships an empty baseTableProperties map and an
        // empty preExistingDeletesByDataFile map -- there is no handle shape that carries CoW
        // planning state for a MoR merge.
        IcebergMergeTableHandle handle = new IcebergMergeTableHandle(
                baseTableHandle(),
                baseWritableTableHandle(),
                RowLevelOperationMode.MERGE_ON_READ,
                ImmutableMap.of(),
                ImmutableMap.of());

        assertThat(handle.getBaseTableProperties()).isEmpty();
        assertThat(handle.getPreExistingDeletesByDataFile()).isEmpty();

        IcebergMergeTableHandle actual = codec().fromJson(codec().toJson(handle));
        assertThat(actual.getBaseTableProperties()).isEmpty();
        assertThat(actual.getPreExistingDeletesByDataFile()).isEmpty();
    }

    @Test
    void preExistingDeletesAreDeepCopiedAndImmutable()
    {
        // Coordinator builds the handle from a map that callers must be free to reuse or mutate
        // afterwards. Both the outer map and the per-data-file lists must therefore be frozen
        // copies so worker rewrite inputs cannot be tampered with after the handle is built.
        DeleteFile firstDelete = samplePositionDeleteFile("s3://bucket/deletes/pos-a.parquet");
        DeleteFile addedAfterConstruction = samplePositionDeleteFile("s3://bucket/deletes/pos-b.parquet");

        List<DeleteFile> mutableDeletes = new ArrayList<>();
        mutableDeletes.add(firstDelete);
        Map<String, List<DeleteFile>> mutableMap = new HashMap<>();
        mutableMap.put("s3://bucket/data/file-a.parquet", mutableDeletes);

        IcebergMergeTableHandle handle = new IcebergMergeTableHandle(
                baseTableHandle(),
                baseWritableTableHandle(),
                RowLevelOperationMode.COPY_ON_WRITE,
                ImmutableMap.of(),
                mutableMap);

        // Mutating the source list and map after construction must not leak into the handle.
        mutableDeletes.add(addedAfterConstruction);
        mutableMap.put("s3://bucket/data/file-x.parquet", List.of(addedAfterConstruction));

        assertThat(handle.getPreExistingDeletesByDataFile())
                .containsOnlyKeys("s3://bucket/data/file-a.parquet");
        assertThat(handle.getPreExistingDeletesByDataFile().get("s3://bucket/data/file-a.parquet"))
                .containsExactly(firstDelete);

        // Returned map and inner list must reject mutation.
        assertThatThrownBy(() -> handle.getPreExistingDeletesByDataFile().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> handle.getPreExistingDeletesByDataFile()
                .get("s3://bucket/data/file-a.parquet").clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static JsonCodec<IcebergMergeTableHandle> codec()
    {
        JsonMapper jsonMapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .get();
        return new JsonCodecFactory(jsonMapper).jsonCodec(IcebergMergeTableHandle.class);
    }

    private static IcebergTableHandle baseTableHandle()
    {
        return new IcebergTableHandle(
                "schema_name",
                "table_name",
                TableType.DATA,
                OptionalLong.of(42L),
                SchemaParser.toJson(SCHEMA),
                OptionalInt.of(UNPARTITIONED.specId()),
                ImmutableMap.of(UNPARTITIONED.specId(), PartitionSpecParser.toJson(UNPARTITIONED)),
                2,
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                "s3://bucket/table",
                ImmutableMap.of(),
                Optional.empty(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
    }

    private static IcebergWritableTableHandle baseWritableTableHandle()
    {
        return new IcebergWritableTableHandle(
                new SchemaTableName("schema_name", "table_name"),
                SchemaParser.toJson(SCHEMA),
                ImmutableMap.of(UNPARTITIONED.specId(), PartitionSpecParser.toJson(UNPARTITIONED)),
                UNPARTITIONED.specId(),
                ImmutableList.of(),
                SortOrder.unsorted().orderId(),
                ImmutableList.of(),
                "s3://bucket/table/data",
                IcebergFileFormat.PARQUET,
                ImmutableMap.of());
    }

    private static DeleteFile samplePositionDeleteFile(String path)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                path,
                FileFormat.PARQUET,
                3,
                64,
                List.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static DeleteFile sampleDeletionVector(String path, String referencedDataFile)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                path,
                FileFormat.PUFFIN,
                1,
                32,
                List.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.of(0),
                Optional.of(32),
                Optional.of(referencedDataFile));
    }
}
