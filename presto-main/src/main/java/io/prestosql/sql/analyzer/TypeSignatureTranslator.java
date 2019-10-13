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
package io.prestosql.sql.analyzer;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.DataType;
import io.prestosql.sql.tree.DataTypeParameter;
import io.prestosql.sql.tree.DateTimeDataType;
import io.prestosql.sql.tree.GenericDataType;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.IntervalDayTimeDataType;
import io.prestosql.sql.tree.NumericParameter;
import io.prestosql.sql.tree.RowDataType;
import io.prestosql.sql.tree.TypeParameter;

import java.util.List;
import java.util.Locale;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TypeSignatureParameter.namedTypeParameter;
import static io.prestosql.spi.type.TypeSignatureParameter.numericParameter;
import static io.prestosql.spi.type.TypeSignatureParameter.typeParameter;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.prestosql.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static java.lang.String.format;

public class TypeSignatureTranslator
{
    private static final SqlParser PARSER = new SqlParser();

    private TypeSignatureTranslator() {}

    public static DataType toSqlType(Type type)
    {
        // TODO: convert Type -> TypeSignature and translate to DataType by walking the tree
        return PARSER.createType(type.getDisplayName());
    }

    public static TypeSignature toTypeSignature(DataType type)
    {
        if (type instanceof DateTimeDataType) {
            return toTypeSignature((DateTimeDataType) type);
        }
        else if (type instanceof IntervalDayTimeDataType) {
            return toTypeSignature((IntervalDayTimeDataType) type);
        }
        else if (type instanceof RowDataType) {
            return toTypeSignature((RowDataType) type);
        }
        else if (type instanceof GenericDataType) {
            return toTypeSignature((GenericDataType) type);
        }

        throw new UnsupportedOperationException("Unsupported DataType: " + type.getClass().getName());
    }

    private static TypeSignature toTypeSignature(GenericDataType type)
    {
        ImmutableList.Builder<TypeSignatureParameter> parameters = ImmutableList.builder();
        for (DataTypeParameter parameter : type.getArguments()) {
            if (parameter instanceof NumericParameter) {
                String value = ((NumericParameter) parameter).getValue();
                try {
                    parameters.add(numericParameter(Long.parseLong(value)));
                }
                catch (NumberFormatException e) {
                    throw semanticException(TYPE_MISMATCH, parameter, "Invalid type parameter: %s", value);
                }
            }
            else if (parameter instanceof TypeParameter) {
                parameters.add(typeParameter(toTypeSignature(((TypeParameter) parameter).getValue())));
            }
            else {
                throw new UnsupportedOperationException("Unsupported type parameter kind: " + parameter.getClass().getName());
            }
        }

        return new TypeSignature(canonicalize(type.getName()), parameters.build());
    }

    private static TypeSignature toTypeSignature(RowDataType type)
    {
        List<TypeSignatureParameter> parameters = type.getFields().stream()
                .map(field -> namedTypeParameter(new NamedTypeSignature(
                        field.getName()
                                .map(TypeSignatureTranslator::canonicalize)
                                .map(value -> new RowFieldName(value, false)),
                        toTypeSignature(field.getType()))))
                .collect(toImmutableList());

        return new TypeSignature(StandardTypes.ROW, parameters);
    }

    private static TypeSignature toTypeSignature(IntervalDayTimeDataType type)
    {
        if (type.getFrom() == IntervalDayTimeDataType.Field.YEAR && type.getTo() == IntervalDayTimeDataType.Field.MONTH) {
            return INTERVAL_YEAR_MONTH.getTypeSignature();
        }

        if (type.getFrom() == IntervalDayTimeDataType.Field.DAY && type.getTo() == IntervalDayTimeDataType.Field.SECOND) {
            return INTERVAL_DAY_TIME.getTypeSignature();
        }

        throw new PrestoException(NOT_SUPPORTED, format("INTERVAL %s TO %s type not supported", type.getFrom(), type.getTo()));
    }

    private static TypeSignature toTypeSignature(DateTimeDataType type)
    {
        boolean withTimeZone = type.isWithTimeZone();

        if (type.getPrecision().isPresent()) {
            throw new PrestoException(NOT_SUPPORTED, String.format("%s type with non-default precision not yet supported", type.getType()));
        }

        switch (type.getType()) {
            case TIMESTAMP:
                if (withTimeZone) {
                    return TIMESTAMP_WITH_TIME_ZONE.getTypeSignature();
                }
                return TIMESTAMP.getTypeSignature();
            case TIME:
                if (withTimeZone) {
                    return TIME_WITH_TIME_ZONE.getTypeSignature();
                }
                return TIME.getTypeSignature();
        }

        throw new UnsupportedOperationException("Unknown dateTime type: " + type.getType());
    }

    private static String canonicalize(Identifier identifier)
    {
        if (identifier.isDelimited()) {
            return identifier.getValue();
        }

        return identifier.getValue().toLowerCase(Locale.ENGLISH); // TODO: make this toUpperCase to match standard SQL semantics
    }
}
