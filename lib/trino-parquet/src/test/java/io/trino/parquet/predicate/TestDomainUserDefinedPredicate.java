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
package io.trino.parquet.predicate;

import io.trino.parquet.predicate.TupleDomainParquetPredicate.DomainUserDefinedPredicate;
import io.trino.spi.predicate.Domain;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

/**
 * {@link DomainUserDefinedPredicate#canDrop} is the consumer of the shared statistics decoding which parquet-mr calls
 * itself, through {@link TupleDomainParquetPredicate#toParquetFilter}, so it is reached without any of the wrapping the
 * other three entry points have.
 */
public class TestDomainUserDefinedPredicate
{
    private static final ColumnDescriptor FLOAT_COLUMN = new ColumnDescriptor(
            new String[] {"FloatColumn"},
            Types.required(FLOAT).named("FloatColumn"),
            0,
            0);

    @Test
    public void testCanDropOnTypeMismatchedColumn()
    {
        // a float column read as varchar decodes to Float, which used to reach the varchar branch and be cast to Slice
        DomainUserDefinedPredicate<Float> predicate = new DomainUserDefinedPredicate<>(
                FLOAT_COLUMN,
                Domain.singleValue(createUnboundedVarcharType(), utf8Slice("abc")),
                UTC);

        assertThat(predicate.canDrop(new Statistics<>(1.0f, 2.0f, Comparator.<Float>naturalOrder()))).isFalse();
    }

    @Test
    public void testCanDropOnMatchingColumn()
    {
        DomainUserDefinedPredicate<Float> predicate = new DomainUserDefinedPredicate<>(
                FLOAT_COLUMN,
                Domain.singleValue(REAL, (long) floatToRawIntBits(9.0f)),
                UTC);

        assertThat(predicate.canDrop(new Statistics<>(1.0f, 2.0f, Comparator.<Float>naturalOrder()))).isTrue();
    }
}
