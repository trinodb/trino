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

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.RowType;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.ChannelComputer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FixedBucketTableShuffleFunctionTest
{
    @Test
    public void testBucketKeyPageUsesBucketKeyColumnPositions()
            throws Exception
    {
        TableSchema schema = schemaWithNonPrefixBucketKey();
        FixedBucketTableShuffleFunction function = new FixedBucketTableShuffleFunction(
                List.of(BIGINT, INTEGER),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                3);

        Page bucketKeyPage = new Page(
                2,
                new LongArrayBlock(2, Optional.empty(), new long[] {10, 11}),
                new IntArrayBlock(2, Optional.empty(), new int[] {4, 5}));

        assertThat(function.getBucket(bucketKeyPage, 0))
                .isEqualTo(expectedPartition(schema, bucketKeyPage, 0, 3));
        assertThat(function.getBucket(bucketKeyPage, 1))
                .isEqualTo(expectedPartition(schema, bucketKeyPage, 1, 3));
    }

    @Test
    public void testPartitionedBucketKeyPageUsesPaimonPartitionAndBucketChannelSelection()
            throws Exception
    {
        TableSchema schema = partitionedSchemaWithNonPrefixBucketKey();
        FixedBucketTableShuffleFunction function = new FixedBucketTableShuffleFunction(
                List.of(INTEGER, BIGINT, INTEGER),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                8);

        Page partitionAndBucketKeyPage = new Page(
                2,
                new IntArrayBlock(2, Optional.empty(), new int[] {20250624, 20250625}),
                new LongArrayBlock(2, Optional.empty(), new long[] {10, 10}),
                new IntArrayBlock(2, Optional.empty(), new int[] {4, 4}));

        assertThat(function.getBucket(partitionAndBucketKeyPage, 0))
                .isEqualTo(expectedPartition(schema, partitionAndBucketKeyPage, 0, 8));
        assertThat(function.getBucket(partitionAndBucketKeyPage, 1))
                .isEqualTo(expectedPartition(schema, partitionAndBucketKeyPage, 1, 8));
        assertThat(function.getBucket(partitionAndBucketKeyPage, 0))
                .isNotEqualTo(function.getBucket(partitionAndBucketKeyPage, 1));
    }

    @Test
    public void testRowIdPageProjectsBucketKeyFromPrimaryKeyColumns()
            throws Exception
    {
        TableSchema schema = schemaWithNonPrefixBucketKey();
        RowType rowIdType = RowType.from(List.of(
                RowType.field("orderkey", BIGINT),
                RowType.field("linenumber", INTEGER)));
        FixedBucketTableShuffleFunction function = new FixedBucketTableShuffleFunction(
                List.of(rowIdType),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                3);
        Page primaryKeyPage = new Page(
                1,
                new LongArrayBlock(1, Optional.empty(), new long[] {10}),
                new IntArrayBlock(1, Optional.empty(), new int[] {4}));
        Page rowIdPage = new Page(RowBlock.fromFieldBlocks(1, new Block[] {
                primaryKeyPage.getBlock(0),
                primaryKeyPage.getBlock(1),
        }));

        assertThat(function.getBucket(rowIdPage, 0))
                .isEqualTo(expectedPartition(schema, primaryKeyPage, 0, 3));
    }

    @Test
    public void testRowIdPageUsesPrimaryKeyFieldOrder()
            throws Exception
    {
        TableSchema schema = schemaWithPrimaryKeyOrderDifferentFromBucketKeyOrder();
        RowType rowIdType = RowType.from(List.of(
                RowType.field("linenumber", INTEGER),
                RowType.field("orderkey", BIGINT)));
        FixedBucketTableShuffleFunction function = new FixedBucketTableShuffleFunction(
                List.of(rowIdType),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                3);
        Page primaryKeyPage = new Page(
                1,
                new IntArrayBlock(1, Optional.empty(), new int[] {4}),
                new LongArrayBlock(1, Optional.empty(), new long[] {10}));
        Page rowIdPage = new Page(RowBlock.fromFieldBlocks(1, new Block[] {
                primaryKeyPage.getBlock(0),
                primaryKeyPage.getBlock(1),
        }));

        assertThat(function.getBucket(rowIdPage, 0))
                .isEqualTo(expectedPartition(schema, primaryKeyPage.getColumns(1, 0), 0, 3));
    }

    @Test
    public void testPartitionedRowIdPageUsesPaimonPartitionAndBucketChannelSelection()
            throws Exception
    {
        TableSchema schema = partitionedSchemaWithNonPrefixBucketKey();
        RowType rowIdType = RowType.from(List.of(
                RowType.field("dt", INTEGER),
                RowType.field("orderkey", BIGINT),
                RowType.field("linenumber", INTEGER)));
        FixedBucketTableShuffleFunction function = new FixedBucketTableShuffleFunction(
                List.of(rowIdType),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                8);
        Page primaryKeyPage = new Page(
                2,
                new IntArrayBlock(2, Optional.empty(), new int[] {20250624, 20250625}),
                new LongArrayBlock(2, Optional.empty(), new long[] {10, 10}),
                new IntArrayBlock(2, Optional.empty(), new int[] {4, 4}));
        Page rowIdPage = new Page(RowBlock.fromFieldBlocks(2, new Block[] {
                primaryKeyPage.getBlock(0),
                primaryKeyPage.getBlock(1),
                primaryKeyPage.getBlock(2),
        }));

        assertThat(function.getBucket(rowIdPage, 0))
                .isEqualTo(expectedPartition(schema, primaryKeyPage, 0, 8));
        assertThat(function.getBucket(rowIdPage, 1))
                .isEqualTo(expectedPartition(schema, primaryKeyPage, 1, 8));
        assertThat(function.getBucket(rowIdPage, 0))
                .isNotEqualTo(function.getBucket(rowIdPage, 1));
    }

    @Test
    public void testRowIdTypeMustMatchPrimaryKeyFields()
            throws Exception
    {
        TableSchema schema = schemaWithNonPrefixBucketKey();

        assertThatThrownBy(() -> new FixedBucketTableShuffleFunction(
                List.of(RowType.anonymous(List.of(BIGINT, INTEGER))),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                3))
                .hasMessage("Paimon row id field at index 0 must be named");

        assertThatThrownBy(() -> new FixedBucketTableShuffleFunction(
                List.of(RowType.from(List.of(
                        RowType.field("linenumber", INTEGER),
                        RowType.field("orderkey", BIGINT)))),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                3))
                .hasMessage("Paimon row id field at index 0 must be primary key 'orderkey', got 'linenumber'");

        assertThatThrownBy(() -> new FixedBucketTableShuffleFunction(
                List.of(RowType.from(List.of(
                        RowType.field("orderkey", INTEGER),
                        RowType.field("linenumber", INTEGER)))),
                new PaimonPartitioningHandle(InstantiationUtil.serializeObject(schema)),
                3))
                .hasMessage("Paimon row id field 'orderkey' type must match Paimon primary key type BIGINT NOT NULL, got integer");
    }

    @Test
    public void testConstructorRejectsMalformedInputs()
            throws Exception
    {
        PaimonPartitioningHandle handle = new PaimonPartitioningHandle(
                InstantiationUtil.serializeObject(schemaWithNonPrefixBucketKey()));

        assertThatThrownBy(() -> new FixedBucketTableShuffleFunction(null, handle, 3))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionChannelTypes is null");
        assertThatThrownBy(() -> new FixedBucketTableShuffleFunction(singletonList(null), handle, 3))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitionChannelTypes contains null type");
        assertThatThrownBy(() -> new FixedBucketTableShuffleFunction(List.of(BIGINT, INTEGER), null, 3))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("partitioningHandle is null");
        assertThatThrownBy(() -> new FixedBucketTableShuffleFunction(List.of(BIGINT, INTEGER), handle, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("workerCount must be positive: 0");
    }

    private static int expectedPartition(TableSchema schema, Page partitionAndBucketKeyPage, int position, int workerCount)
    {
        PaimonRow row = new PaimonRow(
                partitionAndBucketKeyPage.getSingleValuePage(position),
                RowKind.INSERT,
                partitionAndBucketKeyTypes(schema),
                partitionAndBucketLogicalTypes(schema));
        org.apache.paimon.types.RowType inputType = schema.projectedLogicalRowType(partitionAndBucketKeys(schema));
        BinaryRow partition = CodeGenUtils.newProjection(inputType, schema.partitionKeys()).apply(row);
        BinaryRow bucketKey = CodeGenUtils.newProjection(inputType, schema.bucketKeys()).apply(row);
        int bucket = BucketFunction.create(new CoreOptions(schema.options()), schema.logicalBucketKeyType())
                .bucket(bucketKey, new CoreOptions(schema.options()).bucket());
        return ChannelComputer.select(partition, bucket, workerCount);
    }

    private static List<io.trino.spi.type.Type> partitionAndBucketKeyTypes(TableSchema schema)
    {
        return partitionAndBucketKeys(schema).stream()
                .map(fieldName -> schema.logicalRowType().getField(fieldName).type())
                .map(PaimonTypeUtils::fromPaimonType)
                .toList();
    }

    private static List<DataType> partitionAndBucketLogicalTypes(TableSchema schema)
    {
        return partitionAndBucketKeys(schema).stream()
                .map(fieldName -> schema.logicalRowType().getField(fieldName).type())
                .toList();
    }

    private static List<String> partitionAndBucketKeys(TableSchema schema)
    {
        return Stream.concat(schema.partitionKeys().stream(), schema.bucketKeys().stream())
                .toList();
    }

    private static TableSchema schemaWithNonPrefixBucketKey()
    {
        return TableSchema.create(1, new Schema(
                DataTypes.ROW(
                                DataTypes.FIELD(0, "orderkey", DataTypes.BIGINT()),
                                DataTypes.FIELD(1, "partkey", DataTypes.BIGINT()),
                                DataTypes.FIELD(2, "suppkey", DataTypes.BIGINT()),
                                DataTypes.FIELD(3, "linenumber", DataTypes.INT()))
                        .getFields(),
                List.of(),
                List.of("orderkey", "linenumber"),
                Map.of(
                        CoreOptions.BUCKET.key(), "7",
                        CoreOptions.BUCKET_KEY.key(), "orderkey,linenumber"),
                ""));
    }

    private static TableSchema schemaWithPrimaryKeyOrderDifferentFromBucketKeyOrder()
    {
        return TableSchema.create(1, new Schema(
                DataTypes.ROW(
                                DataTypes.FIELD(0, "orderkey", DataTypes.BIGINT()),
                                DataTypes.FIELD(1, "linenumber", DataTypes.INT()))
                        .getFields(),
                List.of(),
                List.of("linenumber", "orderkey"),
                Map.of(
                        CoreOptions.BUCKET.key(), "7",
                        CoreOptions.BUCKET_KEY.key(), "orderkey,linenumber"),
                ""));
    }

    private static TableSchema partitionedSchemaWithNonPrefixBucketKey()
    {
        return TableSchema.create(1, new Schema(
                DataTypes.ROW(
                                DataTypes.FIELD(0, "dt", DataTypes.INT()),
                                DataTypes.FIELD(1, "orderkey", DataTypes.BIGINT()),
                                DataTypes.FIELD(2, "partkey", DataTypes.BIGINT()),
                                DataTypes.FIELD(3, "linenumber", DataTypes.INT()))
                        .getFields(),
                List.of("dt"),
                List.of("dt", "orderkey", "linenumber"),
                Map.of(
                        CoreOptions.BUCKET.key(), "7",
                        CoreOptions.BUCKET_KEY.key(), "orderkey,linenumber"),
                ""));
    }
}
