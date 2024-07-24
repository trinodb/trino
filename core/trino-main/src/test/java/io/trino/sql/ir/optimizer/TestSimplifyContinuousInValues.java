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
package io.trino.sql.ir.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.rule.SimplifyContinuousInValues;
import io.trino.type.Reals;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.testing.TestingSession.testSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyContinuousInValues
{
    @Test
    void test()
    {
        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of(new Reference(BIGINT, "a"), new Constant(BIGINT, 1L)))))
                .describedAs("non-constant list")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of())))
                .describedAs("empty list")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of(new Constant(BIGINT, 1L)))))
                .describedAs("single value list")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, 1L)))))
                .describedAs("null value, single value list")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of(new Constant(BIGINT, null), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))))
                .describedAs("continuous values with null")
                .isEqualTo(Optional.of(or(
                        new IsNull(new Reference(BIGINT, "x")),
                        new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L), new Constant(BIGINT, 2L)))));

        assertThat(optimize(
                new In(new Reference(BIGINT, "x"), ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L), new Constant(BIGINT, 3L)))))
                .describedAs("non-null continuous values")
                .isEqualTo(Optional.of(
                        new Between(new Reference(BIGINT, "x"), new Constant(BIGINT, 1L), new Constant(BIGINT, 3L))));

        assertThat(optimize(
                new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                new Constant(BIGINT, 1L),
                                new Constant(BIGINT, 2L),
                                new Constant(BIGINT, 2L),
                                new Constant(BIGINT, 3L)))))
                .describedAs("repeated continuous values")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(
                                new Constant(BIGINT, 1L),
                                new Constant(BIGINT, 2L),
                                new Constant(BIGINT, 4L),
                                new Constant(BIGINT, 5L)))))
                .describedAs("discontinuous values")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(
                        new Reference(BIGINT, "x"),
                        ImmutableList.of(new Constant(BIGINT, Long.MAX_VALUE), new Constant(BIGINT, Long.MIN_VALUE)))))
                .describedAs("overflow handling")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(new Reference(DOUBLE, "x"), ImmutableList.of(new Constant(DOUBLE, 1.0), new Constant(DOUBLE, 2.0), new Constant(DOUBLE, 3.0)))))
                .describedAs("unsupported type")
                .isEqualTo(Optional.empty());

        assertThat(optimize(
                new In(
                        new Reference(REAL, "x"),
                        ImmutableList.of(
                                new Constant(REAL, Reals.toReal(1.0f)),
                                new Constant(REAL, Reals.toReal(2.0f)),
                                new Constant(REAL, Reals.toReal(3.0f))))))
                .describedAs("unsupported type")
                .isEqualTo(Optional.empty());
    }

    @Test
    public void verifySupportedTypeAssumptions()
    {
        for (Type type : supportedTypes()) {
            assertThat(type.getJavaType()).isEqualTo(long.class);

            // Continuous logical values for the Type
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 201);
            Object currentValue = 0L;
            for (int i = 0; i < 100; i++) {
                writeNativeValue(type, blockBuilder, currentValue);
                currentValue = type.getNextValue(currentValue).orElseThrow(() -> new RuntimeException("type " + type));
            }
            Block block = blockBuilder.build();

            // Verify that values are continuous in the representation space as well
            boolean areRepresentationValuesContinuous = true;
            for (int i = 0; i < block.getPositionCount() - 1; i++) {
                if ((type.getLong(block, i) + 1) != type.getLong(block, i + 1)) {
                    areRepresentationValuesContinuous = false;
                    break;
                }
            }

            List<Expression> valuesList = IntStream.range(0, block.getPositionCount())
                    .mapToObj(i -> new Constant(type, type.getLong(block, i)))
                    .collect(toImmutableList());
            In in = new In(new Reference(type, "x"), valuesList);
            if (areRepresentationValuesContinuous) {
                assertThat(optimize(in))
                        .isEqualTo(Optional.of(new Between(
                                new Reference(type, "x"),
                                new Constant(type, type.getLong(block, 0)),
                                new Constant(type, type.getLong(block, block.getPositionCount() - 1)))));
            }
            else {
                assertThat(optimize(in)).isEmpty();
            }
        }
    }

    private static List<Type> supportedTypes()
    {
        return ImmutableList.of(
                INTEGER,
                BIGINT,
                SMALLINT,
                TINYINT,
                TIMESTAMP_SECONDS,
                TIMESTAMP_MILLIS,
                TIMESTAMP_MICROS,
                TIME_SECONDS,
                TIME_MILLIS,
                TIME_MICROS,
                TIME_NANOS,
                TIME_PICOS,
                DATE,
                createDecimalType(5, 0),
                createDecimalType(7, 2));
    }

    private static Optional<Expression> optimize(Expression expression)
    {
        return new SimplifyContinuousInValues().apply(expression, testSession(), ImmutableMap.of());
    }
}
