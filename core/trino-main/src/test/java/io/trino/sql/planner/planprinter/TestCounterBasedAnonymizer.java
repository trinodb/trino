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
package io.trino.sql.planner.planprinter;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.type.UnknownType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCounterBasedAnonymizer
{
    @Test
    public void testTimestampWithTimeZoneValueAnonymization()
    {
        CounterBasedAnonymizer anonymizer = new CounterBasedAnonymizer();
        assertThat(anonymizer.anonymize(TIMESTAMP_TZ_MILLIS, "2012-10-30 18:00:00.000 America/Los_Angeles"))
                .isEqualTo("timestamp_3_with_time_zone_value_1");
    }

    @Test
    public void testSymbolReferenceAnonymization()
    {
        Logical expression = new Logical(AND, ImmutableList.of(
                new Comparison(GREATER_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 1L)),
                new Comparison(LESS_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 2L)),
                new Comparison(EQUAL, new Reference(INTEGER, "c"), new Constant(INTEGER, 3L))));
        CounterBasedAnonymizer anonymizer = new CounterBasedAnonymizer();
        assertThat(anonymizer.anonymize(expression))
                .isEqualTo("((\"symbol_1\" > 'integer_literal_1') AND (\"symbol_2\" < 'integer_literal_2') AND (\"symbol_3\" = 'integer_literal_3'))");
    }

    @Test
    public void testLiteralAnonymization()
    {
        CounterBasedAnonymizer anonymizer = new CounterBasedAnonymizer();
//
//        assertThat(anonymizer.anonymize(GenericLiteral.constant(VarcharType.VARCHAR, Slices.utf8Slice("abc"))))
//                .isEqualTo("'varchar_literal_1'");
//
//        assertThat(anonymizer.anonymize(GenericLiteral.constant(BIGINT, 1L)))
//                .isEqualTo("'bigint_literal_2'");
//
//        assertThat(anonymizer.anonymize(TRUE_LITERAL))
//                .isEqualTo("true");

        assertThat(anonymizer.anonymize(new Constant(UnknownType.UNKNOWN, null)))
                .isEqualTo("null");
    }
}
