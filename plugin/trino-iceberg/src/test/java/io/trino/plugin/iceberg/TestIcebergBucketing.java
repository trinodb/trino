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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.UUIDType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static io.trino.plugin.iceberg.PartitionTransforms.getBucketTransform;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.String.format;
import static org.apache.iceberg.types.Type.TypeID.DECIMAL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestIcebergBucketing
{
    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();

    @Test
    public void testBucketNumberCompare()
    {
        // Only primitive types are covered here (logical types like timestamp, date or time map to long)
        assertBucketAndHashEquals("string", "Trino rocks", 2131833594);

        assertBucketAndHashEquals("int", null, null);
        assertBucketAndHashEquals("int", 0, 1669671676);
        assertBucketAndHashEquals("int", 300_000, 1798339266);
        assertBucketAndHashEquals("int", Integer.MIN_VALUE, 74448856);
        assertBucketAndHashEquals("int", Integer.MAX_VALUE, 1819228606);

        assertBucketAndHashEquals("long", null, null);
        assertBucketAndHashEquals("long", 0L, 1669671676);
        assertBucketAndHashEquals("long", 300_000_000_000L, 371234369);
        assertBucketAndHashEquals("long", Long.MIN_VALUE, 1366273829);
        assertBucketAndHashEquals("long", Long.MAX_VALUE, 40977599);

        assertBucketAndHashEquals("string", null, null);
        assertBucketAndHashEquals("string", "", 0);
        assertBucketAndHashEquals("string", "test string", 671244848);
        assertBucketAndHashEquals("string", "\u5f3a\u5927\u7684Trino\u5f15\u64ce", 822296301); // 3-byte UTF-8 sequences (in Basic Plane, i.e. Plane 0)
        assertBucketAndHashEquals("string", "\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF", 775615312); // 4 code points: 20FFC - 20FFF. 4-byte UTF-8 sequences in Supplementary Plane 2

        assertBucketAndHashEquals("decimal(1, 1)", null, null);
        assertBucketAndHashEquals("decimal(1, 0)", "0", 1364076727);
        assertBucketAndHashEquals("decimal(1, 0)", "1", 1683673515);
        assertBucketAndHashEquals("decimal(1, 0)", "9", 1771774483);
        assertBucketAndHashEquals("decimal(3, 1)", "0.1", 1683673515);
        assertBucketAndHashEquals("decimal(3, 1)", "1.0", 307159211);
        assertBucketAndHashEquals("decimal(3, 1)", "12.3", 1308316337);
        assertBucketAndHashEquals("decimal(18, 10)", "0", 1364076727);
        assertBucketAndHashEquals("decimal(38, 10)", "999999.9999999999", 1053577599);
        assertBucketAndHashEquals("decimal(38, 0)", "99999999999999999999999999999999999999", 1067515814);
        assertBucketAndHashEquals("decimal(38, 10)", "9999999999999999999999999999.9999999999", 1067515814);
        assertBucketAndHashEquals("decimal(38, 10)", "123456789012345.0", 93815101);

        assertBucketAndHashEquals("binary", null, null);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap(new byte[]{}), 0);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap("hello trino".getBytes(StandardCharsets.UTF_8)), 493441885);
        assertBucketAndHashEquals("binary", ByteBuffer.wrap("\uD843\uDFFC\uD843\uDFFD\uD843\uDFFE\uD843\uDFFF".getBytes(StandardCharsets.UTF_16)), 1291558121);
    }

    @Test(dataProvider = "unsupportedBucketingTypes")
    public void testUnsupportedTypes(Type type)
    {
        assertThatThrownBy(() -> computeIcebergBucket(type, null, 1))
                .hasMessage(format("Cannot bucket by type: %s", type));

        assertThatThrownBy(() -> computeTrinoBucket(type, null, 1))
                .hasMessage(format("Unsupported type for 'bucket': %s", toTrinoType(type, TYPE_MANAGER)));
    }

    @Test(dataProvider = "unsupportedTrinoBucketingTypes")
    public void testUnsupportedTrinoTypes(Type type)
    {
        assertThatThrownBy(() -> computeTrinoBucket(type, null, 1))
                .hasMessage(format("Cannot convert from Iceberg type '%s' (%s) to Trino type", type, type.typeId()));

        assertNull(computeIcebergBucket(type, null, 1));
    }

    @DataProvider
    public Object[][] unsupportedBucketingTypes()
    {
        return new Object[][] {
            {BooleanType.get()},
            {FloatType.get()},
            {DoubleType.get()}};
    }

    @DataProvider
    public Object[][] unsupportedTrinoBucketingTypes()
    {
        return new Object[][] {
                {UUIDType.get()}};
    }

    private void assertBucketAndHashEquals(String icebergTypeName, Object value, Integer expectedHashValue)
    {
        Type icebergType = Types.fromPrimitiveString(icebergTypeName);
        if (value != null && icebergType.typeId() == DECIMAL) {
            value = new BigDecimal((String) value).setScale(((DecimalType) icebergType).scale());
        }

        assertBucketEquals(icebergType, value);
        assertHashEquals(icebergType, value, expectedHashValue);
    }

    private void assertBucketEquals(Type icebergType, Object value)
    {
        assertBucketNumberEquals(icebergType, value, Integer.MAX_VALUE);

        assertBucketNumberEquals(icebergType, value, 2);
        assertBucketNumberEquals(icebergType, value, 7);
        assertBucketNumberEquals(icebergType, value, 31);
        assertBucketNumberEquals(icebergType, value, 32);
        assertBucketNumberEquals(icebergType, value, 100);
        assertBucketNumberEquals(icebergType, value, 10000);
        assertBucketNumberEquals(icebergType, value, 524287); // prime number
        assertBucketNumberEquals(icebergType, value, 1 << 30);
    }

    private void assertHashEquals(Type icebergType, Object value, Integer expectedHashValue)
    {
        Integer icebergBucketHash = computeIcebergBucket(icebergType, value, Integer.MAX_VALUE);
        Integer trinoBucketHash = computeTrinoBucket(icebergType, value, Integer.MAX_VALUE);

        assertEquals(
                icebergBucketHash,
                expectedHashValue,
                format("expected Iceberg %s(%s) hash to be %d, got %d", icebergType, value, expectedHashValue, icebergBucketHash));

        assertEquals(
                trinoBucketHash,
                expectedHashValue,
                format("expected Trino %s(%s) hash to be %d, got %d", icebergType, value, expectedHashValue, trinoBucketHash));
    }

    private void assertBucketNumberEquals(Type icebergType, Object value, int bucketCount)
    {
        Integer icebergBucket = computeIcebergBucket(icebergType, value, bucketCount);
        Integer trinoBucket = computeTrinoBucket(icebergType, value, bucketCount);

        assertEquals(
                icebergBucket,
                trinoBucket,
                format("icebergType=%s, icebergBucket=%d, trinoBucket=%d", icebergType, icebergBucket, trinoBucket));
    }

    private Integer computeTrinoBucket(Type icebergType, Object value, int bucketCount)
    {
        io.trino.spi.type.Type trinoType = toTrinoType(icebergType, TYPE_MANAGER);
        Function<Block, Block> bucketTransform = getBucketTransform(trinoType, bucketCount);

        BlockBuilder blockBuilder = trinoType.createBlockBuilder(null, 1);

        if (value != null) {
            if (isLongDecimal(trinoType)) {
                value = Decimals.encodeScaledValue((BigDecimal) value, ((io.trino.spi.type.DecimalType) trinoType).getScale());
            }

            if (isShortDecimal(trinoType)) {
                value = Decimals.encodeShortScaledValue((BigDecimal) value, ((io.trino.spi.type.DecimalType) trinoType).getScale());
            }

            if (trinoType instanceof VarbinaryType) {
                value = ((ByteBuffer) value).array();
            }
        }

        writeNativeValue(trinoType, blockBuilder, value);
        Block block = blockBuilder.build();

        Block bucketBlock = bucketTransform.apply(block);
        return bucketBlock.isNull(0) ? null : bucketBlock.getInt(0, 0);
    }

    private Integer computeIcebergBucket(Type type, Object value, int bucketCount)
    {
        Transform<Object, Integer> bucketTransform = Transforms.bucket(type, bucketCount);
        assertTrue(bucketTransform.canTransform(type), format("bucket function %s is not able to transform type %s", bucketTransform, type));
        return bucketTransform.apply(value);
    }
}
