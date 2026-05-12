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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.system.PartitionsTable;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitionsTableBuildBatchFilter
{
    @Test
    public void testEmptyBatchReturnsAlwaysFalse()
    {
        Schema schema = new Schema(required(1, "dt", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").build();
        Expression filter = PartitionsTable.buildBatchFilter(
                ImmutableList.of(), spec.fields(), schema, partitionTypes(spec));
        assertThat(filter).isEqualTo(Expressions.alwaysFalse());
    }

    @Test
    public void testSingleIdentityPartition()
    {
        Schema schema = new Schema(required(1, "dt", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, 19358),
                wrap(spec, 19359),
                wrap(spec, 19360));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        UnboundPredicate<?> pred = asInstanceOf(UnboundPredicate.class, filter);
        assertThat(pred.op()).isEqualTo(Expression.Operation.IN);
        assertThat(pred.ref().name()).isEqualTo("dt");
        assertThat(pred.literals()).hasSize(3);
    }

    @Test
    public void testMultiColumnIdentity()
    {
        Schema schema = new Schema(
                required(1, "region", Types.StringType.get()),
                required(2, "dt", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("region")
                .identity("dt")
                .build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, "us", 19358),
                wrap(spec, "eu", 19359));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        And and = asInstanceOf(And.class, filter);
        UnboundPredicate<?> left = asInstanceOf(UnboundPredicate.class, and.left());
        UnboundPredicate<?> right = asInstanceOf(UnboundPredicate.class, and.right());
        assertThat(left.op()).isEqualTo(Expression.Operation.IN);
        assertThat(right.op()).isEqualTo(Expression.Operation.IN);
        assertThat(Set.of(left.ref().name(), right.ref().name())).isEqualTo(Set.of("region", "dt"));
    }

    @Test
    public void testAllNullForFieldReturnsIsNull()
    {
        Schema schema = new Schema(optional(1, "dt", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, (Object) null),
                wrap(spec, (Object) null));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        UnboundPredicate<?> pred = asInstanceOf(UnboundPredicate.class, filter);
        assertThat(pred.op()).isEqualTo(Expression.Operation.IS_NULL);
        assertThat(pred.ref().name()).isEqualTo("dt");
    }

    @Test
    public void testMixedNullAndNonNullReturnsOr()
    {
        Schema schema = new Schema(optional(1, "dt", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("dt").build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, 19358),
                wrap(spec, (Object) null),
                wrap(spec, 19359));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        Or or = asInstanceOf(Or.class, filter);
        UnboundPredicate<?> left = asInstanceOf(UnboundPredicate.class, or.left());
        UnboundPredicate<?> right = asInstanceOf(UnboundPredicate.class, or.right());
        assertThat(left.op()).isEqualTo(Expression.Operation.IN);
        assertThat(right.op()).isEqualTo(Expression.Operation.IS_NULL);
        assertThat(left.literals()).hasSize(2);
    }

    @Test
    public void testAllBucketPartitioningReturnsAlwaysTrue()
    {
        Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 4).build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, 0),
                wrap(spec, 1));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        // Non-identity partition fields are skipped; no conjuncts -> alwaysTrue.
        assertThat(filter).isEqualTo(Expressions.alwaysTrue());
    }

    @Test
    public void testMixedIdentityAndBucketFiltersOnlyIdentity()
    {
        Schema schema = new Schema(
                required(1, "dt", Types.DateType.get()),
                required(2, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("dt")
                .bucket("id", 4)
                .build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, 19358, 0),
                wrap(spec, 19358, 1));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        UnboundPredicate<?> pred = asInstanceOf(UnboundPredicate.class, filter);
        assertThat(pred.op()).isEqualTo(Expression.Operation.IN);
        assertThat(pred.ref().name()).isEqualTo("dt");
        assertThat(pred.literals()).hasSize(1);
    }

    @Test
    public void testNestedIdentityUsesDottedSourceName()
    {
        Schema schema = new Schema(required(
                1,
                "r1",
                Types.StructType.of(required(2, "f1", Types.IntegerType.get()))));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("r1.f1").build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, 1),
                wrap(spec, 2));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        UnboundPredicate<?> pred = asInstanceOf(UnboundPredicate.class, filter);
        assertThat(pred.op()).isEqualTo(Expression.Operation.IN);
        assertThat(pred.ref().name()).isEqualTo("r1.f1");
    }

    @Test
    public void testUnpartitionedReturnsAlwaysTrue()
    {
        Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(wrap(spec));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        assertThat(filter).isEqualTo(Expressions.alwaysTrue());
    }

    @Test
    public void testVoidTransformReturnsAlwaysTrue()
    {
        // void (alwaysNull) maps every value to null. Like other non-identity transforms, we skip
        // the field in filter construction; the in-memory tuple check in getStatisticsForBatch
        // preserves correctness.
        Schema schema = new Schema(optional(1, "dt", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).alwaysNull("dt").build();
        List<StructLikeWrapperWithFieldIdToIndex> batch = ImmutableList.of(
                wrap(spec, (Object) null),
                wrap(spec, (Object) null));

        Expression filter = PartitionsTable.buildBatchFilter(
                batch, spec.fields(), schema, partitionTypes(spec));

        assertThat(filter).isEqualTo(Expressions.alwaysTrue());
    }

    private static StructLikeWrapperWithFieldIdToIndex wrap(PartitionSpec spec, Object... values)
    {
        Types.StructType structType = spec.partitionType();
        Record record = GenericRecord.create(structType);
        for (int i = 0; i < values.length; i++) {
            record.set(i, values[i]);
        }
        StructLikeWrapper wrapper = StructLikeWrapper.forType(structType).set(record);
        return new StructLikeWrapperWithFieldIdToIndex(wrapper, structType);
    }

    private static List<Type> partitionTypes(PartitionSpec spec)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (PartitionField field : spec.fields()) {
            Type.PrimitiveType sourceType = (Type.PrimitiveType) spec.schema().findField(field.sourceId()).type();
            types.add(field.transform().getResultType(sourceType));
        }
        return types.build();
    }

    private static <T> T asInstanceOf(Class<T> type, Object value)
    {
        assertThat(value).isInstanceOf(type);
        return type.cast(value);
    }
}
