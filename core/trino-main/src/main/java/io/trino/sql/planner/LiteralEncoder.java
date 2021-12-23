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
import io.airlift.slice.SliceUtf8;
import io.trino.Session;
import io.trino.block.BlockSerdeUtil;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.scalar.VarbinaryFunctions;
import io.trino.operator.scalar.timestamp.TimestampToVarcharCast;
import io.trino.operator.scalar.timestamptz.TimestampWithTimeZoneToVarcharCast;
import io.trino.spi.block.Block;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.TimestampLiteral;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.LiteralFunction.LITERAL_FUNCTION_NAME;
import static io.trino.metadata.LiteralFunction.typeForMagicLiteral;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.type.DateTimes.parseTimestampWithTimeZone;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class LiteralEncoder
{
    private final PlannerContext plannerContext;

    public LiteralEncoder(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    public List<Expression> toExpressions(Session session, List<?> objects, List<? extends Type> types)
    {
        requireNonNull(objects, "objects is null");
        requireNonNull(types, "types is null");
        checkArgument(objects.size() == types.size(), "objects and types do not have the same size");

        ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
        for (int i = 0; i < objects.size(); i++) {
            Object object = objects.get(i);
            Type type = types.get(i);
            expressions.add(toExpression(session, object, type));
        }
        return expressions.build();
    }

    public Expression toExpression(Session session, Object object, Type type)
    {
        requireNonNull(type, "type is null");

        if (object instanceof Expression) {
            return (Expression) object;
        }

        if (object == null) {
            if (type.equals(UNKNOWN)) {
                return new NullLiteral();
            }
            return new Cast(new NullLiteral(), toSqlType(type), false, true);
        }

        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(object), "object.getClass (%s) and type.getJavaType (%s) do not agree", object.getClass(), type.getJavaType());

        if (type.equals(TINYINT)) {
            return new GenericLiteral("TINYINT", object.toString());
        }

        if (type.equals(SMALLINT)) {
            return new GenericLiteral("SMALLINT", object.toString());
        }

        if (type.equals(INTEGER)) {
            return new LongLiteral(object.toString());
        }

        if (type.equals(BIGINT)) {
            LongLiteral expression = new LongLiteral(object.toString());
            if (expression.getValue() >= Integer.MIN_VALUE && expression.getValue() <= Integer.MAX_VALUE) {
                return new GenericLiteral("BIGINT", object.toString());
            }
            return new LongLiteral(object.toString());
        }

        if (type.equals(DOUBLE)) {
            Double value = (Double) object;
            // WARNING: the ORC predicate code depends on NaN and infinity not appearing in a tuple domain, so
            // if you remove this, you will need to update the TupleDomainOrcPredicate
            // When changing this, don't forget about similar code for REAL below
            if (value.isNaN()) {
                return FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                        .setName(QualifiedName.of("nan"))
                        .build();
            }
            if (value.equals(Double.NEGATIVE_INFINITY)) {
                return ArithmeticUnaryExpression.negative(FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                        .setName(QualifiedName.of("infinity"))
                        .build());
            }
            if (value.equals(Double.POSITIVE_INFINITY)) {
                return FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                        .setName(QualifiedName.of("infinity"))
                        .build();
            }
            return new DoubleLiteral(object.toString());
        }

        if (type.equals(REAL)) {
            Float value = intBitsToFloat(((Long) object).intValue());
            // WARNING for ORC predicate code as above (for double)
            if (value.isNaN()) {
                return new Cast(
                        FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                                .setName(QualifiedName.of("nan"))
                                .build(),
                        toSqlType(REAL));
            }
            if (value.equals(Float.NEGATIVE_INFINITY)) {
                return ArithmeticUnaryExpression.negative(new Cast(
                        FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                                .setName(QualifiedName.of("infinity"))
                                .build(),
                        toSqlType(REAL)));
            }
            if (value.equals(Float.POSITIVE_INFINITY)) {
                return new Cast(
                        FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                                .setName(QualifiedName.of("infinity"))
                                .build(),
                        toSqlType(REAL));
            }
            return new GenericLiteral("REAL", value.toString());
        }

        if (type instanceof DecimalType) {
            String string;
            if (isShortDecimal(type)) {
                string = Decimals.toString((long) object, ((DecimalType) type).getScale());
            }
            else {
                string = Decimals.toString((Int128) object, ((DecimalType) type).getScale());
            }
            return new Cast(new DecimalLiteral(string), toSqlType(type));
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            Slice value = (Slice) object;
            if (varcharType.isUnbounded()) {
                return new GenericLiteral("VARCHAR", value.toStringUtf8());
            }
            StringLiteral stringLiteral = new StringLiteral(value.toStringUtf8());
            int boundedLength = varcharType.getBoundedLength();
            int valueLength = SliceUtf8.countCodePoints(value);
            if (boundedLength == valueLength) {
                return stringLiteral;
            }
            if (boundedLength > valueLength) {
                return new Cast(stringLiteral, toSqlType(type), false, true);
            }
            throw new IllegalArgumentException(format("Value [%s] does not fit in type %s", value.toStringUtf8(), varcharType));
        }

        if (type instanceof CharType) {
            StringLiteral stringLiteral = new StringLiteral(((Slice) object).toStringUtf8());
            return new Cast(stringLiteral, toSqlType(type), false, true);
        }

        if (type.equals(BOOLEAN)) {
            return new BooleanLiteral(object.toString());
        }

        if (type.equals(DATE)) {
            return new GenericLiteral("DATE", new SqlDate(toIntExact((Long) object)).toString());
        }

        if (type instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) type;
            String representation;
            if (timestampType.isShort()) {
                representation = TimestampToVarcharCast.cast(timestampType.getPrecision(), (Long) object).toStringUtf8();
            }
            else {
                representation = TimestampToVarcharCast.cast(timestampType.getPrecision(), (LongTimestamp) object).toStringUtf8();
            }
            return new TimestampLiteral(representation);
        }

        if (type instanceof TimestampWithTimeZoneType) {
            TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
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
                return new TimestampLiteral(representation);
            }
        }

        // There is no automatic built in encoding for this Trino type,
        // so instead the stack type is encoded as another Trino type.

        // If the stack value is not a simple type, encode the stack value in a block
        if (!type.getJavaType().isPrimitive() && type.getJavaType() != Slice.class && type.getJavaType() != Block.class) {
            object = nativeValueToBlock(type, object);
        }

        if (object instanceof Block) {
            SliceOutput output = new DynamicSliceOutput(toIntExact(((Block) object).getSizeInBytes()));
            BlockSerdeUtil.writeBlock(plannerContext.getBlockEncodingSerde(), output, (Block) object);
            object = output.slice();
            // This if condition will evaluate to true: object instanceof Slice && !type.equals(VARCHAR)
        }

        Type argumentType = typeForMagicLiteral(type);
        Expression argument;
        if (object instanceof Slice) {
            // HACK: we need to serialize VARBINARY in a format that can be embedded in an expression to be
            // able to encode it in the plan that gets sent to workers.
            // We do this by transforming the in-memory varbinary into a call to from_base64(<base64-encoded value>)
            Slice encoded = VarbinaryFunctions.toBase64((Slice) object);
            argument = FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                    .setName(QualifiedName.of("from_base64"))
                    .addArgument(VARCHAR, new StringLiteral(encoded.toStringUtf8()))
                    .build();
        }
        else {
            argument = toExpression(session, object, argumentType);
        }

        ResolvedFunction resolvedFunction = plannerContext.getMetadata().getCoercion(session, QualifiedName.of(LITERAL_FUNCTION_NAME), argumentType, type);
        return FunctionCallBuilder.resolve(session, plannerContext.getMetadata())
                .setName(resolvedFunction.toQualifiedName())
                .addArgument(argumentType, argument)
                .build();
    }
}
