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
package io.prestosql.type;

import com.google.common.collect.ImmutableSet;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.RowType.anonymousRow;
import static io.prestosql.spi.type.RowType.field;
import static io.prestosql.spi.type.RowType.rowType;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.type.JoniRegexpType.JONI_REGEXP;
import static io.prestosql.type.JsonPathType.JSON_PATH;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.type.Re2JRegexpType.RE2J_REGEXP;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypeCoercion
{
    private final Metadata metadata = createTestMetadataManager();
    private final TypeCoercion typeCoercion = new TypeCoercion(metadata::getType);

    @Test
    public void testIsTypeOnlyCoercion()
    {
        assertTrue(typeCoercion.isTypeOnlyCoercion(BIGINT, BIGINT));
        assertTrue(typeCoercion.isTypeOnlyCoercion(createVarcharType(42), createVarcharType(44)));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createVarcharType(44), createVarcharType(42)));

        assertFalse(typeCoercion.isTypeOnlyCoercion(createCharType(42), createVarcharType(42)));

        assertTrue(typeCoercion.isTypeOnlyCoercion(new ArrayType(createVarcharType(42)), new ArrayType(createVarcharType(44))));
        assertFalse(typeCoercion.isTypeOnlyCoercion(new ArrayType(createVarcharType(44)), new ArrayType(createVarcharType(42))));

        assertTrue(typeCoercion.isTypeOnlyCoercion(createDecimalType(22, 1), createDecimalType(23, 1)));
        assertTrue(typeCoercion.isTypeOnlyCoercion(createDecimalType(2, 1), createDecimalType(3, 1)));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createDecimalType(23, 1), createDecimalType(22, 1)));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createDecimalType(3, 1), createDecimalType(2, 1)));
        assertFalse(typeCoercion.isTypeOnlyCoercion(createDecimalType(3, 1), createDecimalType(22, 1)));

        assertTrue(typeCoercion.isTypeOnlyCoercion(new ArrayType(createDecimalType(22, 1)), new ArrayType(createDecimalType(23, 1))));
        assertTrue(typeCoercion.isTypeOnlyCoercion(new ArrayType(createDecimalType(2, 1)), new ArrayType(createDecimalType(3, 1))));
        assertFalse(typeCoercion.isTypeOnlyCoercion(new ArrayType(createDecimalType(23, 1)), new ArrayType(createDecimalType(22, 1))));
        assertFalse(typeCoercion.isTypeOnlyCoercion(new ArrayType(createDecimalType(3, 1)), new ArrayType(createDecimalType(2, 1))));

        assertTrue(typeCoercion.isTypeOnlyCoercion(
                mapType(createDecimalType(2, 1), createDecimalType(2, 1)),
                mapType(createDecimalType(2, 1), createDecimalType(3, 1))));

        assertFalse(typeCoercion.isTypeOnlyCoercion(
                mapType(createDecimalType(2, 1), createDecimalType(2, 1)),
                mapType(createDecimalType(2, 1), createDecimalType(23, 1))));

        assertFalse(typeCoercion.isTypeOnlyCoercion(
                mapType(createDecimalType(2, 1), createDecimalType(2, 1)),
                mapType(createDecimalType(2, 1), createDecimalType(3, 2))));

        assertTrue(typeCoercion.isTypeOnlyCoercion(
                mapType(createDecimalType(2, 1), createDecimalType(2, 1)),
                mapType(createDecimalType(3, 1), createDecimalType(3, 1))));

        assertFalse(typeCoercion.isTypeOnlyCoercion(
                mapType(createDecimalType(3, 1), createDecimalType(3, 1)),
                mapType(createDecimalType(2, 1), createDecimalType(2, 1))));
    }

    private Type mapType(Type keyType, Type valueType)
    {
        return metadata.getType(TypeSignature.mapType(keyType.getTypeSignature(), valueType.getTypeSignature()));
    }

    @Test
    public void testTypeCompatibility()
    {
        assertThat(UNKNOWN, UNKNOWN).hasCommonSuperType(UNKNOWN).canCoerceToEachOther();
        assertThat(BIGINT, BIGINT).hasCommonSuperType(BIGINT).canCoerceToEachOther();
        assertThat(UNKNOWN, BIGINT).hasCommonSuperType(BIGINT).canCoerceFirstToSecondOnly();

        assertThat(BIGINT, DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();
        assertThat(DATE, TIMESTAMP).hasCommonSuperType(TIMESTAMP).canCoerceFirstToSecondOnly();
        assertThat(DATE, TIMESTAMP_WITH_TIME_ZONE).hasCommonSuperType(TIMESTAMP_WITH_TIME_ZONE).canCoerceFirstToSecondOnly();
        assertThat(TIME, TIME_WITH_TIME_ZONE).hasCommonSuperType(TIME_WITH_TIME_ZONE).canCoerceFirstToSecondOnly();
        assertThat(TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE).hasCommonSuperType(TIMESTAMP_WITH_TIME_ZONE).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, JONI_REGEXP).hasCommonSuperType(JONI_REGEXP).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, RE2J_REGEXP).hasCommonSuperType(RE2J_REGEXP).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, LIKE_PATTERN).hasCommonSuperType(LIKE_PATTERN).canCoerceFirstToSecondOnly();
        assertThat(VARCHAR, JSON_PATH).hasCommonSuperType(JSON_PATH).canCoerceFirstToSecondOnly();

        assertThat(REAL, DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();
        assertThat(REAL, TINYINT).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();
        assertThat(REAL, SMALLINT).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();
        assertThat(REAL, INTEGER).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();
        assertThat(REAL, BIGINT).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();

        assertThat(TIMESTAMP, TIME_WITH_TIME_ZONE).isIncompatible();
        assertThat(VARBINARY, VARCHAR).isIncompatible();

        assertThat(UNKNOWN, new ArrayType(BIGINT)).hasCommonSuperType(new ArrayType(BIGINT)).canCoerceFirstToSecondOnly();
        assertThat(new ArrayType(BIGINT), new ArrayType(DOUBLE)).hasCommonSuperType(new ArrayType(DOUBLE)).canCoerceFirstToSecondOnly();
        assertThat(new ArrayType(BIGINT), new ArrayType(UNKNOWN)).hasCommonSuperType(new ArrayType(BIGINT)).canCoerceSecondToFirstOnly();
        assertThat(mapType(BIGINT, DOUBLE), mapType(BIGINT, DOUBLE)).hasCommonSuperType(mapType(BIGINT, DOUBLE)).canCoerceToEachOther();
        assertThat(mapType(BIGINT, DOUBLE), mapType(DOUBLE, DOUBLE)).hasCommonSuperType(mapType(DOUBLE, DOUBLE)).canCoerceFirstToSecondOnly();

        assertThat(
                rowType(field("a", BIGINT), field("b", DOUBLE), field("c", VARCHAR)),
                rowType(field("a", BIGINT), field("b", DOUBLE), field("c", VARCHAR)))
                .hasCommonSuperType(rowType(field("a", BIGINT), field("b", DOUBLE), field("c", VARCHAR)))
                .canCoerceToEachOther();

        assertThat(createDecimalType(22, 1), createDecimalType(23, 1)).hasCommonSuperType(createDecimalType(23, 1)).canCoerceFirstToSecondOnly();
        assertThat(BIGINT, createDecimalType(23, 1)).hasCommonSuperType(createDecimalType(23, 1)).canCoerceFirstToSecondOnly();
        assertThat(BIGINT, createDecimalType(18, 0)).hasCommonSuperType(createDecimalType(19, 0)).cannotCoerceToEachOther();
        assertThat(BIGINT, createDecimalType(19, 0)).hasCommonSuperType(createDecimalType(19, 0)).canCoerceFirstToSecondOnly();
        assertThat(BIGINT, createDecimalType(37, 1)).hasCommonSuperType(createDecimalType(37, 1)).canCoerceFirstToSecondOnly();
        assertThat(REAL, createDecimalType(37, 1)).hasCommonSuperType(REAL).canCoerceSecondToFirstOnly();
        assertThat(new ArrayType(createDecimalType(23, 1)), new ArrayType(createDecimalType(22, 1))).hasCommonSuperType(new ArrayType(createDecimalType(23, 1))).canCoerceSecondToFirstOnly();
        assertThat(new ArrayType(BIGINT), new ArrayType(createDecimalType(2, 1))).hasCommonSuperType(new ArrayType(createDecimalType(20, 1))).cannotCoerceToEachOther();
        assertThat(new ArrayType(BIGINT), new ArrayType(createDecimalType(20, 1))).hasCommonSuperType(new ArrayType(createDecimalType(20, 1))).canCoerceFirstToSecondOnly();

        assertThat(createDecimalType(3, 2), DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();
        assertThat(createDecimalType(22, 1), DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();
        assertThat(createDecimalType(37, 1), DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();
        assertThat(createDecimalType(37, 37), DOUBLE).hasCommonSuperType(DOUBLE).canCoerceFirstToSecondOnly();

        assertThat(createDecimalType(22, 1), REAL).hasCommonSuperType(REAL).canCoerceFirstToSecondOnly();
        assertThat(createDecimalType(3, 2), REAL).hasCommonSuperType(REAL).canCoerceFirstToSecondOnly();
        assertThat(createDecimalType(37, 37), REAL).hasCommonSuperType(REAL).canCoerceFirstToSecondOnly();

        assertThat(INTEGER, createDecimalType(23, 1)).hasCommonSuperType(createDecimalType(23, 1)).canCoerceFirstToSecondOnly();
        assertThat(INTEGER, createDecimalType(9, 0)).hasCommonSuperType(createDecimalType(10, 0)).cannotCoerceToEachOther();
        assertThat(INTEGER, createDecimalType(10, 0)).hasCommonSuperType(createDecimalType(10, 0)).canCoerceFirstToSecondOnly();
        assertThat(INTEGER, createDecimalType(37, 1)).hasCommonSuperType(createDecimalType(37, 1)).canCoerceFirstToSecondOnly();

        assertThat(TINYINT, createDecimalType(2, 0)).hasCommonSuperType(createDecimalType(3, 0)).cannotCoerceToEachOther();
        assertThat(TINYINT, createDecimalType(9, 0)).hasCommonSuperType(createDecimalType(9, 0)).canCoerceFirstToSecondOnly();
        assertThat(TINYINT, createDecimalType(2, 1)).hasCommonSuperType(createDecimalType(4, 1)).cannotCoerceToEachOther();
        assertThat(TINYINT, createDecimalType(3, 0)).hasCommonSuperType(createDecimalType(3, 0)).canCoerceFirstToSecondOnly();
        assertThat(TINYINT, createDecimalType(37, 1)).hasCommonSuperType(createDecimalType(37, 1)).canCoerceFirstToSecondOnly();

        assertThat(SMALLINT, createDecimalType(37, 1)).hasCommonSuperType(createDecimalType(37, 1)).canCoerceFirstToSecondOnly();
        assertThat(SMALLINT, createDecimalType(4, 0)).hasCommonSuperType(createDecimalType(5, 0)).cannotCoerceToEachOther();
        assertThat(SMALLINT, createDecimalType(5, 0)).hasCommonSuperType(createDecimalType(5, 0)).canCoerceFirstToSecondOnly();
        assertThat(SMALLINT, createDecimalType(2, 0)).hasCommonSuperType(createDecimalType(5, 0)).cannotCoerceToEachOther();
        assertThat(SMALLINT, createDecimalType(9, 0)).hasCommonSuperType(createDecimalType(9, 0)).canCoerceFirstToSecondOnly();
        assertThat(SMALLINT, createDecimalType(2, 1)).hasCommonSuperType(createDecimalType(6, 1)).cannotCoerceToEachOther();

        assertThat(createCharType(42), createCharType(40)).hasCommonSuperType(createCharType(42)).canCoerceSecondToFirstOnly();
        assertThat(createCharType(42), createCharType(44)).hasCommonSuperType(createCharType(44)).canCoerceFirstToSecondOnly();
        assertThat(createVarcharType(42), createVarcharType(42)).hasCommonSuperType(createVarcharType(42)).canCoerceToEachOther();
        assertThat(createVarcharType(42), createVarcharType(44)).hasCommonSuperType(createVarcharType(44)).canCoerceFirstToSecondOnly();
        assertThat(createCharType(40), createVarcharType(42)).hasCommonSuperType(createCharType(42)).cannotCoerceToEachOther();
        assertThat(createCharType(42), createVarcharType(42)).hasCommonSuperType(createCharType(42)).canCoerceSecondToFirstOnly();
        assertThat(createCharType(44), createVarcharType(42)).hasCommonSuperType(createCharType(44)).canCoerceSecondToFirstOnly();

        assertThat(createCharType(42), JONI_REGEXP).hasCommonSuperType(JONI_REGEXP).canCoerceFirstToSecondOnly();
        assertThat(createCharType(42), JSON_PATH).hasCommonSuperType(JSON_PATH).canCoerceFirstToSecondOnly();
        assertThat(createCharType(42), LIKE_PATTERN).hasCommonSuperType(LIKE_PATTERN).canCoerceFirstToSecondOnly();
        assertThat(createCharType(42), RE2J_REGEXP).hasCommonSuperType(RE2J_REGEXP).canCoerceFirstToSecondOnly();

        assertThat(anonymousRow(createVarcharType(2)), anonymousRow(createVarcharType(5)))
                .hasCommonSuperType(anonymousRow(createVarcharType(5)))
                .canCoerceFirstToSecondOnly();

        assertThat(rowType(field("a", INTEGER)), rowType(field("a", BIGINT)))
                .hasCommonSuperType(rowType(field("a", BIGINT)))
                .canCoerceFirstToSecondOnly();

        assertThat(rowType(field("a", INTEGER)), rowType(field("b", BIGINT)))
                .hasCommonSuperType(anonymousRow(BIGINT))
                .canCoerceFirstToSecondOnly();

        assertThat(anonymousRow(INTEGER), rowType(field("b", BIGINT)))
                .hasCommonSuperType(anonymousRow(BIGINT))
                .canCoerceFirstToSecondOnly();

        assertThat(
                rowType(field("a", INTEGER)),
                rowType(field("a", createVarcharType(2))))
                .isIncompatible();

        assertThat(
                rowType(field("a", INTEGER)),
                rowType(field("a", INTEGER), field("b", createVarcharType(2))))
                .isIncompatible();

        assertThat(
                rowType(field("a", INTEGER), field("b", createVarcharType(2))),
                rowType(field("a", BIGINT), field("b", createVarcharType(5))))
                .hasCommonSuperType(rowType(field("a", BIGINT), field("b", createVarcharType(5))))
                .canCoerceFirstToSecondOnly();

        assertThat(
                rowType(field("a", INTEGER), field("b", createVarcharType(2))),
                rowType(field(BIGINT), field("b", createVarcharType(5))))
                .hasCommonSuperType(rowType(field(BIGINT), field("b", createVarcharType(5))))
                .canCoerceFirstToSecondOnly();

        assertThat(
                rowType(field("a", INTEGER), field("b", createVarcharType(5))),
                rowType(field("c", BIGINT), field("d", createVarcharType(2))))
                .hasCommonSuperType(anonymousRow(BIGINT, createVarcharType(5)))
                .cannotCoerceToEachOther();

        assertThat(
                rowType(field("a", rowType(field("c", INTEGER), field("b", createVarcharType(2))))),
                rowType(field(rowType(field("c", INTEGER), field(createVarcharType(5))))))
                .hasCommonSuperType(rowType(field(rowType(field("c", INTEGER), field(createVarcharType(5))))))
                .canCoerceFirstToSecondOnly();

        assertThat(
                rowType(field("a", rowType(field("c", INTEGER), field("b", createVarcharType(2))))),
                rowType(field("a", rowType(field("c", INTEGER), field("b", createVarcharType(5))))))
                .hasCommonSuperType(rowType(field("a", rowType(field("c", INTEGER), field("b", createVarcharType(5))))))
                .canCoerceFirstToSecondOnly();

        assertThat(
                rowType(field("a", rowType(field("c", INTEGER), field("b", createVarcharType(5))))),
                rowType(field("d", rowType(field("e", INTEGER), field("b", createVarcharType(5))))))
                .hasCommonSuperType(rowType(field(rowType(field(INTEGER), field("b", createVarcharType(5))))))
                .canCoerceToEachOther();
    }

    @Test
    public void testCoerceTypeBase()
    {
        assertEquals(typeCoercion.coerceTypeBase(createDecimalType(21, 1), "decimal"), Optional.of(createDecimalType(21, 1)));
        assertEquals(typeCoercion.coerceTypeBase(BIGINT, "decimal"), Optional.of(createDecimalType(19, 0)));
        assertEquals(typeCoercion.coerceTypeBase(INTEGER, "decimal"), Optional.of(createDecimalType(10, 0)));
        assertEquals(typeCoercion.coerceTypeBase(TINYINT, "decimal"), Optional.of(createDecimalType(3, 0)));
        assertEquals(typeCoercion.coerceTypeBase(SMALLINT, "decimal"), Optional.of(createDecimalType(5, 0)));
    }

    @Test
    public void testCanCoerceIsTransitive()
    {
        Set<Type> types = getStandardPrimitiveTypes();
        for (Type transitiveType : types) {
            for (Type resultType : types) {
                if (typeCoercion.canCoerce(transitiveType, resultType)) {
                    for (Type sourceType : types) {
                        if (typeCoercion.canCoerce(sourceType, transitiveType)) {
                            if (!typeCoercion.canCoerce(sourceType, resultType)) {
                                fail(format("'%s' -> '%s' coercion is missing when transitive coercion is possible: '%s' -> '%s' -> '%s'",
                                        sourceType, resultType, sourceType, transitiveType, resultType));
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testCastOperatorsExistForCoercions()
    {
        Set<Type> types = getStandardPrimitiveTypes();
        for (Type sourceType : types) {
            for (Type resultType : types) {
                if (typeCoercion.canCoerce(sourceType, resultType) && sourceType != UNKNOWN && resultType != UNKNOWN) {
                    try {
                        metadata.getCoercion(sourceType, resultType);
                    }
                    catch (Exception e) {
                        fail(format("'%s' -> '%s' coercion exists but there is no cast operator", sourceType, resultType), e);
                    }
                }
            }
        }
    }

    private Set<Type> getStandardPrimitiveTypes()
    {
        ImmutableSet.Builder<Type> builder = ImmutableSet.builder();
        // add unparametrized types
        builder.addAll(metadata.getTypes());
        // add corner cases for parametrized types
        builder.add(createDecimalType(1, 0));
        builder.add(createDecimalType(17, 0));
        builder.add(createDecimalType(38, 0));
        builder.add(createDecimalType(17, 17));
        builder.add(createDecimalType(38, 38));
        builder.add(createVarcharType(0));
        builder.add(createUnboundedVarcharType());
        builder.add(createCharType(0));
        builder.add(createCharType(42));
        return builder.build();
    }

    private CompatibilityAssertion assertThat(Type firstType, Type secondType)
    {
        Optional<Type> commonSuperType1 = typeCoercion.getCommonSuperType(firstType, secondType);
        Optional<Type> commonSuperType2 = typeCoercion.getCommonSuperType(secondType, firstType);
        assertEquals(commonSuperType1, commonSuperType2, "Expected getCommonSuperType to return the same result when invoked in either order");
        boolean canCoerceFirstToSecond = typeCoercion.canCoerce(firstType, secondType);
        boolean canCoerceSecondToFirst = typeCoercion.canCoerce(secondType, firstType);
        return new CompatibilityAssertion(commonSuperType1, canCoerceFirstToSecond, canCoerceSecondToFirst);
    }

    private class CompatibilityAssertion
    {
        private final Optional<Type> commonSuperType;
        private final boolean canCoerceFirstToSecond;
        private final boolean canCoerceSecondToFirst;

        public CompatibilityAssertion(Optional<Type> commonSuperType, boolean canCoerceFirstToSecond, boolean canCoerceSecondToFirst)
        {
            this.commonSuperType = requireNonNull(commonSuperType, "commonSuperType is null");

            // Assert that: (canFirstCoerceToSecond || canSecondCoerceToFirst) => commonSuperType.isPresent
            assertTrue(!(canCoerceFirstToSecond || canCoerceSecondToFirst) || commonSuperType.isPresent(), "Expected canCoercion to be false when there is no commonSuperType");
            this.canCoerceFirstToSecond = canCoerceFirstToSecond;
            this.canCoerceSecondToFirst = canCoerceSecondToFirst;
        }

        public void isIncompatible()
        {
            assertTrue(!commonSuperType.isPresent(), "Expected to be incompatible");
        }

        public CompatibilityAssertion hasCommonSuperType(Type expected)
        {
            assertTrue(commonSuperType.isPresent(), "Expected commonSuperType to be present");
            assertEquals(commonSuperType.get(), expected, "commonSuperType");
            return this;
        }

        public CompatibilityAssertion canCoerceToEachOther()
        {
            assertTrue(canCoerceFirstToSecond, "Expected first be coercible to second");
            assertTrue(canCoerceSecondToFirst, "Expected second be coercible to first");
            return this;
        }

        public CompatibilityAssertion canCoerceFirstToSecondOnly()
        {
            assertTrue(canCoerceFirstToSecond, "Expected first be coercible to second");
            assertFalse(canCoerceSecondToFirst, "Expected second NOT be coercible to first");
            return this;
        }

        public CompatibilityAssertion canCoerceSecondToFirstOnly()
        {
            assertFalse(canCoerceFirstToSecond, "Expected first NOT be coercible to second");
            assertTrue(canCoerceSecondToFirst, "Expected second be coercible to first");
            return this;
        }

        public CompatibilityAssertion cannotCoerceToEachOther()
        {
            assertFalse(canCoerceFirstToSecond, "Expected first NOT be coercible to second");
            assertFalse(canCoerceSecondToFirst, "Expected second NOT be coercible to first");
            return this;
        }
    }
}
