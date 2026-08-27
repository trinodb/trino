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
package io.trino.plugin.paimon;

import io.airlift.json.JsonCodec;
import io.trino.node.InternalNode;
import io.trino.spi.Node;
import io.trino.spi.NodeVersion;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.type.RowType;
import io.trino.testing.TestingNodeManager;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class TrinoPartitioningHandleTest
{
    private final JsonCodec<PaimonPartitioningHandle> codec = JsonCodec.jsonCodec(PaimonPartitioningHandle.class);

    @Test
    public void testTrinoPartitioningHandle()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData, false, OptionalInt.of(3));
        testRoundTrip(expected);
        assertThat(expected.isSingleNode()).isFalse();
        assertThat(expected.dynamicBucketAssignerParallelism()).hasValue(3);
    }

    @Test
    public void testSingleNodePartitioningHandle()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData, true);

        testRoundTrip(expected);
        assertThat(expected.isSingleNode()).isTrue();
    }

    @Test
    public void testPartitioningHandleRejectsMissingSchema()
    {
        assertThatThrownBy(() -> codec.fromJson("{}"))
                .rootCause()
                .hasMessageContaining("Missing required creator property 'schema'");
    }

    @Test
    public void testPartitioningHandleRejectsEmptySchema()
    {
        assertThatThrownBy(() -> new PaimonPartitioningHandle(new byte[0]))
                .hasMessage("schema is empty");

        assertThatThrownBy(() -> codec.fromJson("{\"schema\":\"\"}"))
                .hasRootCauseMessage("schema is empty");
    }

    @Test
    public void testPartitioningHandleRejectsInvalidDynamicBucketAssignerParallelism()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();

        assertThatThrownBy(() -> new PaimonPartitioningHandle(schemaData, false, OptionalInt.of(0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("dynamicBucketAssignerParallelism must be positive: 0");
        assertThatThrownBy(() -> codec.fromJson("{\"schema\":\"%s\",\"dynamicBucketAssignerParallelism\":0}"
                .formatted(Base64.getEncoder().encodeToString(schemaData))))
                .hasRootCauseMessage("dynamicBucketAssignerParallelism must be positive: 0");
    }

    @Test
    public void testPartitioningHandleRejectsUnknownJsonFields()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        String json = appendJsonField(
                codec.toJson(new PaimonPartitioningHandle(schemaData)),
                "\"unexpectedField\":true");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Unknown PaimonPartitioningHandle JSON field: unexpectedField");
    }

    @Test
    public void testPartitioningHandleAcceptsTrinoTypedJsonField()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"%s\"".formatted(typedHandleId(PaimonPartitioningHandle.class)));

        assertThat(codec.fromJson(json)).isEqualTo(expected);
    }

    @Test
    public void testPartitioningHandleRejectsInvalidTrinoTypedJsonField()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":true");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonPartitioningHandle JSON @type field");
    }

    @Test
    public void testPartitioningHandleRejectsConnectorNameOnlyTypedJsonField()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle expected = new PaimonPartitioningHandle(schemaData);
        String json = appendJsonField(codec.toJson(expected), "\"@type\":\"paimon\"");

        assertThatThrownBy(() -> codec.fromJson(json))
                .hasRootCauseMessage("Invalid PaimonPartitioningHandle JSON @type field");
    }

    @Test
    public void testPartitioningHandleDefensivelyCopiesSchema()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        byte[] expectedSchema = schemaData.clone();
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(schemaData);

        schemaData[0] = (byte) (schemaData[0] + 1);
        byte[] returnedSchema = handle.schema();
        returnedSchema[0] = (byte) (returnedSchema[0] + 1);

        assertThat(handle.schema()).isEqualTo(expectedSchema);
    }

    @Test
    public void testPartitioningHandleRejectsSerializedNonTableSchema()
            throws Exception
    {
        byte[] schemaData = InstantiationUtil.serializeObject("test_schema");

        assertThatThrownBy(() -> new PaimonPartitioningHandle(schemaData))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schema must contain a serialized Paimon TableSchema");
        assertThatThrownBy(() -> codec.fromJson("{\"schema\":\"%s\"}".formatted(Base64.getEncoder().encodeToString(schemaData))))
                .hasRootCauseMessage("schema must contain a serialized Paimon TableSchema");
    }

    @Test
    public void testPartitioningHandleRejectsMalformedSerializedSchema()
    {
        assertThatThrownBy(() -> new PaimonPartitioningHandle(new byte[] {1, 2, 3}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("schema must contain a serialized Paimon TableSchema")
                .hasCauseInstanceOf(IOException.class);

        assertThatThrownBy(() -> codec.fromJson("{\"schema\":\"AQID\"}"))
                .isInstanceOfSatisfying(IllegalArgumentException.class, exception -> {
                    assertThat(exception.getCause()).hasMessageContaining("schema must contain a serialized Paimon TableSchema");
                    assertThat(exception).hasRootCauseInstanceOf(IOException.class);
                });
    }

    @Test
    public void testNodePartitioningProviderRequiresPaimonPartitioningHandle()
            throws Exception
    {
        byte[] schemaData = serializedTestSchema();
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(schemaData);

        assertThat(PaimonNodePartitioningProvider.getPartitioningHandle(handle)).isSameAs(handle);

        assertThatThrownBy(() -> PaimonNodePartitioningProvider.getPartitioningHandle(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitioningHandle is null");

        ConnectorPartitioningHandle wrongHandle = new ConnectorPartitioningHandle() {};
        assertThatThrownBy(() -> PaimonNodePartitioningProvider.getPartitioningHandle(wrongHandle))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Paimon node partitioning requires PaimonPartitioningHandle, got: %s",
                        wrongHandle.getClass().getName());
    }

    @Test
    public void testNodePartitioningProviderRejectsMalformedInputs()
            throws Exception
    {
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.create());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(serializedTestSchema());

        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, null, 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionChannelTypes is null");
        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, singletonList(null), 1))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionChannelTypes contains null type");
        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, List.of(BIGINT), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bucketCount must be positive: 0");
        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, List.of(BIGINT), -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("bucketCount must be positive: -1");
    }

    @Test
    public void testSingleNodePartitioningProviderRoutesAllRowsToSingleBucket()
            throws Exception
    {
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.create());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(serializedTestSchema(), true);

        assertThat(provider.getBucketNodeMapping(null, null, handle))
                .get()
                .extracting(mapping -> mapping.getBucketCount())
                .isEqualTo(1);
        assertThat(provider.getBucketFunction(null, null, handle, List.of(BIGINT), 8)
                .getBucket(null, 0))
                .isEqualTo(0);
    }

    @Test
    public void testDynamicBucketPartitioningProviderUsesFixedWorkerAssignerMapping()
            throws Exception
    {
        Node nodeB = node("node-b", "127.0.0.2");
        Node nodeA = node("node-a", "127.0.0.1");
        Node nodeC = node("node-c", "127.0.0.3");
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(
                TestingNodeManager.builder().doNotScheduleOnCoordinator().addNodes(List.of(nodeB, nodeA, nodeC)).build());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(InstantiationUtil.serializeObject(
                dynamicBucketSchema(Map.of(CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM.key(), "2"))));

        assertThat(provider.getBucketNodeMapping(null, null, handle))
                .get()
                .satisfies(mapping -> {
                    assertThat(mapping.getBucketCount()).isEqualTo(2);
                    assertThat(mapping.hasFixedMapping()).isTrue();
                    assertThat(mapping.getFixedMapping())
                            .extracting(Node::getNodeIdentifier)
                            .containsExactly("node-a", "node-b");
                });
    }

    @Test
    public void testDynamicBucketPartitioningProviderUsesPlannedAssignerMapping()
            throws Exception
    {
        Node nodeB = node("node-b", "127.0.0.2");
        Node nodeA = node("node-a", "127.0.0.1");
        Node nodeC = node("node-c", "127.0.0.3");
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(
                TestingNodeManager.builder().doNotScheduleOnCoordinator().addNodes(List.of(nodeB, nodeA, nodeC)).build());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(
                InstantiationUtil.serializeObject(dynamicBucketSchema(Map.of())),
                false,
                OptionalInt.of(2));

        assertThat(provider.getBucketNodeMapping(null, null, handle))
                .get()
                .satisfies(mapping -> {
                    assertThat(mapping.getBucketCount()).isEqualTo(2);
                    assertThat(mapping.hasFixedMapping()).isTrue();
                    assertThat(mapping.getFixedMapping())
                            .extracting(Node::getNodeIdentifier)
                            .containsExactly("node-a", "node-b");
                });
    }

    @Test
    public void testDynamicBucketShuffleFunctionUsesConfiguredAssignerCountWithoutPlannedHandle()
            throws Exception
    {
        TableSchema schema = dynamicBucketSchema(Map.of(
                CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM.key(), "2"));
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(
                InstantiationUtil.serializeObject(schema));
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(
                TestingNodeManager.builder().addNodes(List.of(
                        node("node-a", "127.0.0.1"),
                        node("node-b", "127.0.0.2"),
                        node("node-c", "127.0.0.3"))).build());

        assertThat(provider.getBucketNodeMapping(null, null, handle))
                .get()
                .extracting(mapping -> mapping.getBucketCount())
                .isEqualTo(2);

        BucketFunction bucketFunction = provider.getBucketFunction(null, null, handle, List.of(BIGINT, BIGINT), 3);
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(schema);
        for (long partitionValue = 1; partitionValue < 100; partitionValue++) {
            for (long id = 1; id < 100; id++) {
                Page page = new Page(
                        1,
                        writeNativeValue(BIGINT, partitionValue),
                        writeNativeValue(BIGINT, id));
                PaimonRow row = new PaimonRow(
                        page,
                        0,
                        RowKind.INSERT,
                        List.of(BIGINT, BIGINT),
                        List.of(DataTypes.BIGINT(), DataTypes.BIGINT()));
                int expected = BucketAssigner.computeAssigner(
                        extractor.partition(row).hashCode(),
                        extractor.trimmedPrimaryKey(row).hashCode(),
                        2,
                        2);
                int previousRouting = BucketAssigner.computeAssigner(
                        extractor.partition(row).hashCode(),
                        extractor.trimmedPrimaryKey(row).hashCode(),
                        3,
                        3);
                if (expected != previousRouting) {
                    assertThat(bucketFunction.getBucket(page, 0)).isEqualTo(expected);
                    return;
                }
            }
        }
        fail("test data did not exercise configured dynamic-bucket routing");
    }

    @Test
    public void testDynamicBucketPartitioningProviderRejectsPlannedAssignerMappingWithoutEnoughWorkers()
            throws Exception
    {
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(
                TestingNodeManager.builder().doNotScheduleOnCoordinator().addNodes(List.of(node("node-a", "127.0.0.1"))).build());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(
                InstantiationUtil.serializeObject(dynamicBucketSchema(Map.of())),
                false,
                OptionalInt.of(2));

        assertThatThrownBy(() -> provider.getBucketNodeMapping(null, null, handle))
                .hasMessage("Paimon HASH_DYNAMIC planned assigner parallelism 2 exceeds available worker nodes 1");
    }

    @Test
    public void testDynamicBucketPartitioningProviderRejectsTooFewTrinoBuckets()
            throws Exception
    {
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.create());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(
                InstantiationUtil.serializeObject(dynamicBucketSchema(Map.of())),
                false,
                OptionalInt.of(2));

        assertThatThrownBy(() -> provider.getBucketFunction(null, null, handle, List.of(BIGINT, BIGINT), 1))
                .hasMessage("Paimon HASH_DYNAMIC assigner parallelism 2 exceeds Trino bucket count 1");
    }

    @Test
    public void testDynamicBucketShuffleFunctionUsesPaimonAssignerHash()
            throws Exception
    {
        TableSchema schema = dynamicBucketSchema(Map.of());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema));
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.create());
        BucketFunction bucketFunction = provider.getBucketFunction(null, null, handle, List.of(BIGINT, BIGINT), 3);
        Page page = new Page(
                1,
                writeNativeValue(BIGINT, 20260708L),
                writeNativeValue(BIGINT, 11L));
        PaimonRow row = new PaimonRow(
                page,
                0,
                RowKind.INSERT,
                List.of(BIGINT, BIGINT),
                List.of(DataTypes.BIGINT(), DataTypes.BIGINT()));
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(schema);
        int expected = BucketAssigner.computeAssigner(
                extractor.partition(row).hashCode(),
                extractor.trimmedPrimaryKey(row).hashCode(),
                3,
                3);

        assertThat(bucketFunction.getBucket(page, 0)).isEqualTo(expected);
    }

    @Test
    public void testDynamicBucketShuffleFunctionUsesPlannedAssignerCount()
            throws Exception
    {
        TableSchema schema = dynamicBucketSchema(Map.of());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(
                InstantiationUtil.serializeObject(schema),
                false,
                OptionalInt.of(2));
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.create());
        BucketFunction bucketFunction = provider.getBucketFunction(null, null, handle, List.of(BIGINT, BIGINT), 5);
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(schema);

        for (long partitionValue = 1; partitionValue < 100; partitionValue++) {
            for (long id = 1; id < 100; id++) {
                Page page = new Page(
                        1,
                        writeNativeValue(BIGINT, partitionValue),
                        writeNativeValue(BIGINT, id));
                PaimonRow row = new PaimonRow(
                        page,
                        0,
                        RowKind.INSERT,
                        List.of(BIGINT, BIGINT),
                        List.of(DataTypes.BIGINT(), DataTypes.BIGINT()));
                int expected = BucketAssigner.computeAssigner(
                        extractor.partition(row).hashCode(),
                        extractor.trimmedPrimaryKey(row).hashCode(),
                        2,
                        2);
                int unplanned = BucketAssigner.computeAssigner(
                        extractor.partition(row).hashCode(),
                        extractor.trimmedPrimaryKey(row).hashCode(),
                        5,
                        5);
                if (expected != unplanned) {
                    assertThat(bucketFunction.getBucket(page, 0)).isEqualTo(expected);
                    return;
                }
            }
        }
        fail("test data did not exercise a planned assigner routing difference");
    }

    @Test
    public void testDynamicBucketShuffleFunctionUsesRowIdPrimaryKeyFields()
            throws Exception
    {
        TableSchema schema = dynamicBucketSchema(Map.of());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema));
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.create());
        RowType rowIdType = RowType.from(List.of(
                RowType.field("dt", BIGINT),
                RowType.field("id", BIGINT)));
        BucketFunction bucketFunction = provider.getBucketFunction(null, null, handle, List.of(rowIdType), 3);
        Page primaryKeyPage = new Page(
                2,
                bigintBlock(20260708L, 20260709L),
                bigintBlock(11L, 12L));
        RowBlock rowIdBlock = RowBlock.fromFieldBlocks(2, new Block[] {
                primaryKeyPage.getBlock(0),
                primaryKeyPage.getBlock(1),
        });
        Page rowIdPage = new Page(rowIdBlock);
        PaimonRow row = new PaimonRow(
                primaryKeyPage,
                0,
                RowKind.INSERT,
                List.of(BIGINT, BIGINT),
                List.of(DataTypes.BIGINT(), DataTypes.BIGINT()));
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(schema);
        int expected = BucketAssigner.computeAssigner(
                extractor.partition(row).hashCode(),
                extractor.trimmedPrimaryKey(row).hashCode(),
                3,
                3);

        assertThat(bucketFunction.getBucket(rowIdPage, 0)).isEqualTo(expected);

        Block dictionaryRowIdBlock = DictionaryBlock.create(2, rowIdBlock, new int[] {1, 0});
        Page dictionaryRowIdPage = new Page(dictionaryRowIdBlock);
        PaimonRow remappedRow = new PaimonRow(
                primaryKeyPage,
                1,
                RowKind.INSERT,
                List.of(BIGINT, BIGINT),
                List.of(DataTypes.BIGINT(), DataTypes.BIGINT()));
        int remappedExpected = BucketAssigner.computeAssigner(
                extractor.partition(remappedRow).hashCode(),
                extractor.trimmedPrimaryKey(remappedRow).hashCode(),
                3,
                3);

        assertThat(bucketFunction.getBucket(dictionaryRowIdPage, 0)).isEqualTo(remappedExpected);
    }

    @Test
    public void testDynamicBucketShuffleFunctionRejectsMalformedRowIdType()
            throws Exception
    {
        TableSchema schema = dynamicBucketSchema(Map.of());
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema));

        assertThatThrownBy(() -> new DynamicBucketTableShuffleFunction(
                List.of(RowType.anonymous(List.of(BIGINT, BIGINT))),
                handle,
                3))
                .hasMessage("Paimon row id field at index 0 must be named");

        assertThatThrownBy(() -> new DynamicBucketTableShuffleFunction(
                List.of(RowType.from(List.of(
                        RowType.field("id", BIGINT),
                        RowType.field("dt", BIGINT)))),
                handle,
                3))
                .hasMessage("Paimon row id field at index 0 must be primary key 'dt', got 'id'");
    }

    @Test
    public void testDynamicBucketShuffleFunctionUsesInitialBucketsAsNumAssigners()
            throws Exception
    {
        TableSchema schema = dynamicBucketSchema(Map.of(CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS.key(), "1"));
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema));
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.create());
        BucketFunction bucketFunction = provider.getBucketFunction(null, null, handle, List.of(BIGINT, BIGINT), 4);
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(schema);

        for (long partitionValue = 1; partitionValue < 100; partitionValue++) {
            for (long id = 1; id < 100; id++) {
                Page page = new Page(
                        1,
                        writeNativeValue(BIGINT, partitionValue),
                        writeNativeValue(BIGINT, id));
                PaimonRow row = new PaimonRow(
                        page,
                        0,
                        RowKind.INSERT,
                        List.of(BIGINT, BIGINT),
                        List.of(DataTypes.BIGINT(), DataTypes.BIGINT()));
                int expected = BucketAssigner.computeAssigner(
                        extractor.partition(row).hashCode(),
                        extractor.trimmedPrimaryKey(row).hashCode(),
                        4,
                        1);
                int previousRouting = BucketAssigner.computeAssigner(
                        extractor.partition(row).hashCode(),
                        extractor.trimmedPrimaryKey(row).hashCode(),
                        4,
                        4);
                if (expected != previousRouting) {
                    assertThat(bucketFunction.getBucket(page, 0)).isEqualTo(expected);
                    return;
                }
            }
        }
        fail("test data did not exercise a dynamic-bucket.initial-buckets routing difference");
    }

    @Test
    public void testKeyDynamicShuffleUsesFullPrimaryKeyHash()
            throws Exception
    {
        TableSchema schema = keyDynamicSchema(Map.of(
                CoreOptions.DYNAMIC_BUCKET_INITIAL_BUCKETS.key(), "1",
                CoreOptions.DYNAMIC_BUCKET_ASSIGNER_PARALLELISM.key(), "2"));
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(
                InstantiationUtil.serializeObject(schema), false, OptionalInt.of(2));
        PaimonNodePartitioningProvider provider = new PaimonNodePartitioningProvider(TestingNodeManager.builder().addNodes(List.of(
                node("node-a", "127.0.0.1"),
                node("node-b", "127.0.0.2"))).build());

        assertThat(provider.getBucketNodeMapping(null, null, handle)).get()
                .extracting(mapping -> mapping.getBucketCount())
                .isEqualTo(2);

        BucketFunction bucketFunction = provider.getBucketFunction(null, null, handle, List.of(BIGINT), 2);
        Page page = new Page(1, writeNativeValue(BIGINT, 11L));
        Page fullRowPage = new Page(
                1,
                writeNativeValue(BIGINT, 20260711L),
                writeNativeValue(BIGINT, 11L));
        PaimonRow fullRow = new PaimonRow(
                fullRowPage,
                0,
                RowKind.INSERT,
                List.of(BIGINT, BIGINT),
                List.of(DataTypes.BIGINT(), DataTypes.BIGINT()));
        assertThat(bucketFunction.getBucket(page, 0))
                .isEqualTo(Math.abs(new RowPartitionKeyExtractor(schema).trimmedPrimaryKey(fullRow).hashCode() % 2));
    }

    private void testRoundTrip(PaimonPartitioningHandle expected)
    {
        String json = codec.toJson(expected);
        PaimonPartitioningHandle actual = codec.fromJson(json);
        assertThat(actual).isEqualTo(expected);
        assertThat(actual.schema()).isEqualTo(expected.schema());
    }

    private static String appendJsonField(String json, String field)
    {
        return json.substring(0, json.length() - 1) + "," + field + "}";
    }

    private static String typedHandleId(Class<?> handleClass)
    {
        return "paimon:" + handleClass.getName();
    }

    private static Block bigintBlock(long... values)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(values.length);
        for (long value : values) {
            BIGINT.writeLong(builder, value);
        }
        return builder.build();
    }

    private static byte[] serializedTestSchema()
            throws Exception
    {
        return InstantiationUtil.serializeObject(testSchema());
    }

    private static TableSchema testSchema()
    {
        return TableSchema.create(1, new Schema(
                DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT())).getFields(),
                List.of(),
                List.of(),
                Map.of(),
                ""));
    }

    private static TableSchema dynamicBucketSchema(Map<String, String> options)
    {
        return TableSchema.create(1, new Schema(
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.BIGINT()),
                        DataTypes.FIELD(1, "id", DataTypes.BIGINT())).getFields(),
                List.of("dt"),
                List.of("dt", "id"),
                mergeOptions(Map.of(CoreOptions.BUCKET.key(), "-1"), options),
                ""));
    }

    private static TableSchema keyDynamicSchema(Map<String, String> options)
    {
        return TableSchema.create(1, new Schema(
                DataTypes.ROW(
                        DataTypes.FIELD(0, "dt", DataTypes.BIGINT()),
                        DataTypes.FIELD(1, "id", DataTypes.BIGINT())).getFields(),
                List.of("dt"),
                List.of("id"),
                mergeOptions(Map.of(CoreOptions.BUCKET.key(), "-1"), options),
                ""));
    }

    private static Map<String, String> mergeOptions(Map<String, String> first, Map<String, String> second)
    {
        HashMap<String, String> result = new HashMap<>();
        result.putAll(first);
        result.putAll(second);
        return Map.copyOf(result);
    }

    private static Node node(String identifier, String host)
    {
        return new InternalNode(identifier, URI.create("local://%s".formatted(host)), NodeVersion.UNKNOWN, false);
    }
}
