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
package io.trino.plugin.pinot.query;

import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.operator.filter.predicate.BaseRawValueBasedPredicateEvaluator;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleFunction;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.plugin.pinot.query.PinotQueryBuilder.getFilterClause;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.IrUtils.or;
import static io.trino.sql.ir.TestingIr.comparison;
import static java.lang.Float.floatToRawIntBits;
import static org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory.newRawValueBasedEvaluator;
import static org.apache.pinot.sql.parsers.CalciteSqlParser.compileToPinotQuery;
import static org.assertj.core.api.Assertions.assertThat;

class TestPinotQueryBuilder
{
    @Test
    void testOrderedFloatingPointPredicateEvaluation()
    {
        for (Type type : List.of(DOUBLE, REAL)) {
            DataType pinotType = type.equals(DOUBLE) ? DataType.DOUBLE : DataType.FLOAT;
            Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("x", pinotType).build();
            DoubleFunction<Object> nativeValue = type.equals(DOUBLE) ? value -> value : value -> (long) floatToRawIntBits((float) value);
            for (ValueSet values : List.of(
                    ValueSet.ofRanges(Range.lessThan(type, nativeValue.apply(0))),
                    ValueSet.ofRanges(Range.greaterThan(type, nativeValue.apply(0))),
                    ValueSet.of(type, nativeValue.apply(Double.NaN)).complement())) {
                Domain domain = Domain.create(values, false);
                TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(new PinotColumnHandle("x", type), domain));
                PinotQuery query = compileToPinotQuery("SELECT x FROM test WHERE " + getFilterClause(constraint, Optional.empty(), false).orElseThrow());
                new QueryOptimizer().optimize(query, schema);
                RangePredicate predicate = (RangePredicate) RequestContextUtils.getFilter(query.getFilterExpression()).getPredicate();
                BaseRawValueBasedPredicateEvaluator evaluator = newRawValueBasedEvaluator(predicate, pinotType);
                for (double value : List.of(Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0, Double.POSITIVE_INFINITY, Double.NaN)) {
                    boolean matches = type.equals(DOUBLE) ? evaluator.applySV(value) : evaluator.applySV((float) value);
                    assertThat(matches)
                            .as("%s contains %s after Pinot optimization", domain, value)
                            .isEqualTo(domain.includesNullableValue(nativeValue.apply(value)));
                }
            }
        }
    }

    @Test
    void testFloatingPointComplementContainingNaN()
    {
        TestingFunctionResolution functions = new TestingFunctionResolution();
        for (Type type : List.of(DOUBLE, REAL)) {
            Symbol symbol = new Symbol(type, "x");
            Constant zero = type.equals(DOUBLE) ? new Constant(type, 0.0) : new Constant(type, 0L);
            Expression predicate = or(
                    comparison(LESS_THAN, symbol.toSymbolReference(), zero),
                    comparison(GREATER_THAN, symbol.toSymbolReference(), zero));
            DomainTranslator.ExtractionResult extraction = DomainTranslator.getExtractionResult(functions.getPlannerContext(), TEST_SESSION, predicate);
            assertThat(extraction.remainingExpression()).isEqualTo(TRUE);
            Domain domain = extraction.tupleDomain().getDomains().orElseThrow().get(symbol);
            assertThat(domain.getValues().complement().isDiscreteSet()).isTrue();
            TupleDomain<ColumnHandle> constraint = extraction.tupleDomain().transformKeys(column -> new PinotColumnHandle(column.name(), column.type()));
            assertThat(getFilterClause(constraint, Optional.empty(), false))
                    .contains("((\"x\" >= '-Infinity' AND \"x\" < '0.0') OR (\"x\" > '0.0' AND \"x\" <= 'Infinity'))");
        }
    }
}
