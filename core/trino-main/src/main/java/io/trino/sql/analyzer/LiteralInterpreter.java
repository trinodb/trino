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
package io.trino.sql.analyzer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.cache.CacheUtils;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;

import java.util.function.Function;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.type.DateTimes.parseTime;
import static io.trino.type.DateTimes.parseTimeWithTimeZone;
import static io.trino.type.DateTimes.parseTimestamp;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static io.trino.util.DateTimeUtils.parseYearMonthInterval;
import static java.util.Objects.requireNonNull;

public final class LiteralInterpreter
{
    private final PlannerContext plannerContext;
    private final ConnectorSession connectorSession;
    private final InterpretedFunctionInvoker functionInvoker;

    private final Cache<Type, Function<GenericLiteral, Object>> genericLiteralEvaluatorCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));

    public LiteralInterpreter(PlannerContext plannerContext, Session session)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.connectorSession = session.toConnectorSession();
        this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
    }

    public Object evaluate(Expression node, Type type)
    {
        if (!(node instanceof Literal literal)) {
            throw new IllegalArgumentException("node must be a Literal");
        }

        return switch (literal) {
            case BinaryLiteral binaryLiteral -> Slices.wrappedBuffer(binaryLiteral.getValue());
            case BooleanLiteral booleanLiteral -> booleanLiteral.getValue();
            case DecimalLiteral decimalLiteral -> Decimals.parse(decimalLiteral.getValue()).getObject();
            case DoubleLiteral doubleLiteral -> doubleLiteral.getValue();
            case GenericLiteral genericLiteral -> parseGenericLiteral(type, genericLiteral);
            case IntervalLiteral intervalLiteral -> parseIntervalLiteral(intervalLiteral);
            case LongLiteral longLiteral -> longLiteral.getParsedValue();
            case StringLiteral stringLiteral -> utf8Slice(stringLiteral.getValue());
            case NullLiteral nullLiteral -> null;
        };
    }

    private Object parseIntervalLiteral(IntervalLiteral intervalLiteral)
    {
        if (intervalLiteral.isYearToMonth()) {
            return intervalLiteral.getSign().multiplier() * parseYearMonthInterval(intervalLiteral.getValue(), intervalLiteral.getStartField(), intervalLiteral.getEndField());
        }
        return intervalLiteral.getSign().multiplier() * parseDayTimeInterval(intervalLiteral.getValue(), intervalLiteral.getStartField(), intervalLiteral.getEndField());
    }

    private Object parseGenericLiteral(Type type, GenericLiteral node)
    {
        return switch (type) {
            case TimeType unused -> parseTime(node.getValue());
            case TimeWithTimeZoneType value -> parseTimeWithTimeZone(value.getPrecision(), node.getValue());
            case TimestampType value -> parseTimestamp(value.getPrecision(), node.getValue());
            case TimestampWithTimeZoneType value -> parseTimestampWithTimeZone(value.getPrecision(), node.getValue());
            default -> {
                Function<GenericLiteral, Object> evaluator = CacheUtils.uncheckedCacheGet(genericLiteralEvaluatorCache, type, () -> {
                    boolean isJson = JSON.equals(type);
                    ResolvedFunction resolvedFunction;
                    if (isJson) {
                        resolvedFunction = plannerContext.getMetadata().resolveBuiltinFunction("json_parse", fromTypes(VARCHAR));
                    }
                    else {
                        resolvedFunction = plannerContext.getMetadata().getCoercion(VARCHAR, type);
                    }
                    return evaluatedNode -> functionInvoker.invoke(resolvedFunction, connectorSession, ImmutableList.of(utf8Slice(evaluatedNode.getValue())));
                });
                yield evaluator.apply(node);
            }
        };
    }
}
