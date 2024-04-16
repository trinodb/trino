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
package io.trino.cost;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.Symbol;

import java.util.OptionalDouble;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.DIVIDE;
import static io.trino.spi.function.OperatorType.MODULUS;
import static io.trino.spi.function.OperatorType.MULTIPLY;
import static io.trino.spi.function.OperatorType.NEGATION;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.statistics.StatsUtil.toStatsRepresentation;
import static io.trino.util.MoreMath.max;
import static io.trino.util.MoreMath.min;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final PlannerContext plannerContext;

    @Inject
    public ScalarStatsCalculator(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext cannot be null");
    }

    public SymbolStatsEstimate calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session)
    {
        return new Visitor(inputStatistics, session).process(scalarExpression);
    }

    private class Visitor
            extends IrVisitor<SymbolStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;

        Visitor(PlanNodeStatsEstimate input, Session session)
        {
            this.input = input;
            this.session = session;
        }

        @Override
        protected SymbolStatsEstimate visitExpression(Expression node, Void context)
        {
            return SymbolStatsEstimate.unknown();
        }

        @Override
        protected SymbolStatsEstimate visitReference(Reference node, Void context)
        {
            return input.getSymbolStatistics(Symbol.from(node));
        }

        @Override
        protected SymbolStatsEstimate visitConstant(Constant node, Void context)
        {
            Type type = node.type();
            Object value = node.value();
            if (value == null) {
                return nullStatsEstimate();
            }

            OptionalDouble doubleValue = toStatsRepresentation(type, value);
            SymbolStatsEstimate.Builder estimate = SymbolStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
        }

        @Override
        protected SymbolStatsEstimate visitCall(Call node, Void context)
        {
            if (node.function().name().equals(builtinFunctionName(NEGATION))) {
                SymbolStatsEstimate stats = process(node.arguments().getFirst());
                return SymbolStatsEstimate.buildFrom(stats)
                        .setLowValue(-stats.getHighValue())
                        .setHighValue(-stats.getLowValue())
                        .build();
            }
            else if (node.function().name().equals(builtinFunctionName(ADD)) ||
                    node.function().name().equals(builtinFunctionName(SUBTRACT)) ||
                    node.function().name().equals(builtinFunctionName(MULTIPLY)) ||
                    node.function().name().equals(builtinFunctionName(DIVIDE)) ||
                    node.function().name().equals(builtinFunctionName(MODULUS))) {
                return processArithmetic(node);
            }

            Expression value = new IrExpressionInterpreter(node, plannerContext, session).optimize();

            if (value instanceof Constant constant && constant.value() == null) {
                return nullStatsEstimate();
            }

            if (value instanceof Constant) {
                return SymbolStatsEstimate.builder()
                        .setNullsFraction(0)
                        .setDistinctValuesCount(1)
                        .build();
            }

            return SymbolStatsEstimate.unknown();
        }

        @Override
        protected SymbolStatsEstimate visitCast(Cast node, Void context)
        {
            SymbolStatsEstimate sourceStats = process(node.expression());

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (isIntegralType(((Expression) node).type())) {
                // todo handle low/high value changes if range gets narrower due to cast (e.g. BIGINT -> SMALLINT)
                if (isFinite(lowValue)) {
                    lowValue = Math.round(lowValue);
                }
                if (isFinite(highValue)) {
                    highValue = Math.round(highValue);
                }
                if (isFinite(lowValue) && isFinite(highValue)) {
                    double integersInRange = highValue - lowValue + 1;
                    if (!isNaN(distinctValuesCount) && distinctValuesCount > integersInRange) {
                        distinctValuesCount = integersInRange;
                    }
                }
            }

            return SymbolStatsEstimate.builder()
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
        }

        private boolean isIntegralType(Type type)
        {
            if (type instanceof BigintType || type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType) {
                return true;
            }

            if (type instanceof DecimalType decimalType) {
                return decimalType.getScale() == 0;
            }

            return false;
        }

        protected SymbolStatsEstimate processArithmetic(Call node)
        {
            requireNonNull(node, "node is null");
            SymbolStatsEstimate left = process(node.arguments().get(0));
            SymbolStatsEstimate right = process(node.arguments().get(1));
            if (left.isUnknown() || right.isUnknown()) {
                return SymbolStatsEstimate.unknown();
            }

            SymbolStatsEstimate.Builder result = SymbolStatsEstimate.builder()
                    .setAverageRowSize(Math.max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(min(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
                result.setLowValue(NaN)
                        .setHighValue(NaN);
            }
            else if (node.function().name().equals(builtinFunctionName(DIVIDE)) && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (node.function().name().equals(builtinFunctionName(MODULUS))) {
                double maxDivisor = max(abs(rightLow), abs(rightHigh));
                if (leftHigh <= 0) {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(0);
                }
                else if (leftLow >= 0) {
                    result.setLowValue(0)
                            .setHighValue(min(maxDivisor, leftHigh));
                }
                else {
                    result.setLowValue(max(-maxDivisor, leftLow))
                            .setHighValue(min(maxDivisor, leftHigh));
                }
            }
            else {
                double v1 = operate(node.function().name(), leftLow, rightLow);
                double v2 = operate(node.function().name(), leftLow, rightHigh);
                double v3 = operate(node.function().name(), leftHigh, rightLow);
                double v4 = operate(node.function().name(), leftHigh, rightHigh);
                double lowValue = min(v1, v2, v3, v4);
                double highValue = max(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(CatalogSchemaFunctionName function, double left, double right)
        {
            return switch (function) {
                case CatalogSchemaFunctionName name when name.equals(builtinFunctionName(ADD)) -> left + right;
                case CatalogSchemaFunctionName name when name.equals(builtinFunctionName(SUBTRACT)) -> left - right;
                case CatalogSchemaFunctionName name when name.equals(builtinFunctionName(MULTIPLY)) -> left * right;
                case CatalogSchemaFunctionName name when name.equals(builtinFunctionName(DIVIDE)) -> left / right;
                case CatalogSchemaFunctionName name when name.equals(builtinFunctionName(MODULUS)) -> left % right;
                default -> throw new IllegalStateException("Unsupported binary arithmetic operation: " + function);
            };
        }

        @Override
        protected SymbolStatsEstimate visitCoalesce(Coalesce node, Void context)
        {
            requireNonNull(node, "node is null");
            SymbolStatsEstimate result = null;
            for (Expression operand : node.operands()) {
                SymbolStatsEstimate operandEstimates = process(operand);
                if (result != null) {
                    result = estimateCoalesce(result, operandEstimates);
                }
                else {
                    result = operandEstimates;
                }
            }
            return requireNonNull(result, "result is null");
        }

        private SymbolStatsEstimate estimateCoalesce(SymbolStatsEstimate left, SymbolStatsEstimate right)
        {
            // Question to reviewer: do you have a method to check if fraction is empty or saturated?
            if (left.getNullsFraction() == 0) {
                return left;
            }
            if (left.getNullsFraction() == 1.0) {
                return right;
            }
            return SymbolStatsEstimate.builder()
                    .setLowValue(min(left.getLowValue(), right.getLowValue()))
                    .setHighValue(max(left.getHighValue(), right.getHighValue()))
                    .setDistinctValuesCount(left.getDistinctValuesCount() +
                            min(right.getDistinctValuesCount(), input.getOutputRowCount() * left.getNullsFraction()))
                    .setNullsFraction(left.getNullsFraction() * right.getNullsFraction())
                    // TODO check if dataSize estimation method is correct
                    .setAverageRowSize(max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .build();
        }
    }

    private static SymbolStatsEstimate nullStatsEstimate()
    {
        return SymbolStatsEstimate.builder()
                .setDistinctValuesCount(0)
                .setNullsFraction(1)
                .build();
    }
}
