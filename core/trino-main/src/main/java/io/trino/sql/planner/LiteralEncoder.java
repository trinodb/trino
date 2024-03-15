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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.block.BlockSerdeUtil;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.VarbinaryFunctions;
import io.trino.operator.scalar.timestamp.TimestampToVarcharCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToVarcharCast;
import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.ArithmeticUnaryExpression;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.GenericLiteral;
import io.trino.sql.ir.NullLiteral;
import jakarta.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.metadata.LiteralFunction.LITERAL_FUNCTION_NAME;
import static io.trino.metadata.LiteralFunction.typeForMagicLiteral;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class LiteralEncoder
{
    private final PlannerContext plannerContext;

    public LiteralEncoder(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public List<Expression> toExpressions(List<?> objects, List<? extends Type> types)
    {
        requireNonNull(objects, "objects is null");
        requireNonNull(types, "types is null");
        checkArgument(objects.size() == types.size(), "objects and types do not have the same size");

        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
        for (int i = 0; i < objects.size(); i++) {
            Object object = objects.get(i);
            Type type = types.get(i);
            expressions.add(toExpression(object, type));
        }
        return expressions.build();
    }

    public Expression toExpression(@Nullable Object object, Type type)
    {
        requireNonNull(type, "type is null");

        if (object instanceof Expression expression) {
            return expression;
        }

        if (object == null) {
            if (type.equals(UNKNOWN)) {
                return new NullLiteral();
            }
            return new Cast(new NullLiteral(), type, false);
        }

        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(object), "object.getClass (%s) and type.getJavaType (%s) do not agree", object.getClass(), type.getJavaType());

        if (type.equals(BOOLEAN) ||
                type.equals(TINYINT) ||
                type.equals(SMALLINT) ||
                type.equals(INTEGER) ||
                type.equals(BIGINT) ||
                type.equals(DOUBLE) ||
                type.equals(INTERVAL_YEAR_MONTH) ||
                type.equals(INTERVAL_DAY_TIME) ||
                type instanceof DecimalType ||
                type instanceof VarcharType ||
                type instanceof CharType ||
                type instanceof VarbinaryType) {
            return GenericLiteral.constant(type, object);
        }

        if (type.equals(REAL)) {
            Float value = intBitsToFloat(((Long) object).intValue());
            if (value.isNaN()) {
                return new Cast(
                        BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                                .setName("nan")
                                .build(),
                        REAL);
            }
            if (value.equals(Float.NEGATIVE_INFINITY)) {
                return ArithmeticUnaryExpression.negative(new Cast(
                        BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                                .setName("infinity")
                                .build(),
                        REAL));
            }
            if (value.equals(Float.POSITIVE_INFINITY)) {
                return new Cast(
                        BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                                .setName("infinity")
                                .build(),
                        REAL);
            }
            return new GenericLiteral(REAL, value.toString());
        }

        if (type.equals(DATE)) {
            return new GenericLiteral(DATE, new SqlDate(toIntExact((Long) object)).toString());
        }

        if (type instanceof TimestampType timestampType) {
            String representation;
            if (timestampType.isShort()) {
                representation = TimestampToVarcharCast.cast(timestampType.getPrecision(), (Long) object).toStringUtf8();
            }
            else {
                representation = TimestampToVarcharCast.cast(timestampType.getPrecision(), (LongTimestamp) object).toStringUtf8();
            }
            return new GenericLiteral(timestampType, representation);
        }

        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            String representation;
            if (timestampWithTimeZoneType.isShort()) {
                representation = TimestampWithTimeZoneToVarcharCast.cast(timestampWithTimeZoneType.getPrecision(), (long) object).toStringUtf8();
            }
            else {
                representation = TimestampWithTimeZoneToVarcharCast.cast(timestampWithTimeZoneType.getPrecision(), (LongTimestampWithTimeZone) object).toStringUtf8();
            }
            if (!object.equals(parseTimestampWithTimeZone(timestampWithTimeZoneType.getPrecision(), representation))) {
                // Certain (point in time, time zone) pairs cannot be represented as a TIMESTAMP literal, as the literal uses local date/time in given time zone.
                // Thus, during DST backwards change by e.g. 1 hour, the local time is "repeated" twice and thus one local date/time logically corresponds to two
                // points in time, leaving one of them non-referencable.
                // TODO (https://github.com/trinodb/trino/issues/5781) consider treating such values as illegal
            }
            else {
                return new GenericLiteral(timestampWithTimeZoneType, representation);
            }
        }

        // There is no automatic built in encoding for this Trino type,
        // so instead the stack type is encoded as another Trino type.

        // If the stack value is not a simple type, encode the stack value in a block
        if (!type.getJavaType().isPrimitive() && type.getJavaType() != Slice.class && type.getJavaType() != Block.class) {
            object = nativeValueToBlock(type, object);
        }

        if (object instanceof Block block) {
            SliceOutput output = new DynamicSliceOutput(toIntExact(block.getSizeInBytes()));
            BlockSerdeUtil.writeBlock(plannerContext.getBlockEncodingSerde(), output, block);
            object = output.slice();
            // This if condition will evaluate to true: object instanceof Slice && !type.equals(VARCHAR)
        }

        Type argumentType = typeForMagicLiteral(type);
        Expression argument;
        if (object instanceof Slice slice) {
            // HACK: we need to serialize VARBINARY in a format that can be embedded in an expression to be
            // able to encode it in the plan that gets sent to workers.
            // We do this by transforming the in-memory varbinary into a call to from_base64(<base64-encoded value>)
            argument = BuiltinFunctionCallBuilder.resolve(plannerContext.getMetadata())
                    .setName("from_base64")
                    .addArgument(GenericLiteral.constant(VARCHAR, VarbinaryFunctions.toBase64(slice)))
                    .build();
        }
        else {
            argument = toExpression(object, argumentType);
        }

        ResolvedFunction resolvedFunction = plannerContext.getMetadata().getCoercion(builtinFunctionName(LITERAL_FUNCTION_NAME), argumentType, type);
        return ResolvedFunctionCallBuilder.builder(resolvedFunction)
                .addArgument(argument)
                .build();
    }
}
