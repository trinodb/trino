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
package io.trino.plugin.jdbc;

import io.trino.plugin.jdbc.PredicatePushdownController.DomainPushdownResult;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.DoubleFunction;

import static io.trino.plugin.jdbc.PredicatePushdownController.FINITE_FLOATING_POINT_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FLOATING_POINT_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.numberColumnMapping;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_DOUBLE;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_REAL;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

class TestFloatingPointQueryBuilder
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new JdbcMetadataSessionProperties(new JdbcMetadataConfig(), Optional.empty()).getSessionProperties())
            .build();

    @Test
    void testOrderedFloatingPointDomains()
            throws SQLException
    {
        for (Type type : List.of(DOUBLE, REAL)) {
            try (TestingDatabase database = new TestingDatabase()) {
                Connection connection = database.getConnection();
                connection.createStatement().execute("CREATE TABLE \"numbers\" (\"x\" " + type + ")");
                List<Double> values = new ArrayList<>(List.of(Double.NEGATIVE_INFINITY, -1.0, -0.0, 0.0, 1.0, Double.POSITIVE_INFINITY, Double.NaN));
                values.add(null);
                try (PreparedStatement insert = connection.prepareStatement("INSERT INTO \"numbers\" VALUES (?)")) {
                    for (Double value : values) {
                        if (value == null) {
                            insert.setNull(1, Types.DOUBLE);
                        }
                        else {
                            insert.setDouble(1, value);
                        }
                        insert.executeUpdate();
                    }
                }
                DoubleFunction<Object> nativeValue = type.equals(DOUBLE) ? value -> value : value -> (long) floatToRawIntBits((float) value);
                List<ValueSet> domains = List.of(
                        ValueSet.none(type),
                        ValueSet.all(type),
                        ValueSet.of(type, nativeValue.apply(Double.NaN)).complement(),
                        ValueSet.of(type, nativeValue.apply(Double.POSITIVE_INFINITY)),
                        ValueSet.of(type, nativeValue.apply(Double.NEGATIVE_INFINITY)),
                        ValueSet.ofRanges(Range.lessThan(type, nativeValue.apply(0))),
                        ValueSet.ofRanges(Range.greaterThan(type, nativeValue.apply(0))),
                        ValueSet.ofRanges(Range.lessThan(type, nativeValue.apply(0)), Range.greaterThan(type, nativeValue.apply(0))),
                        ValueSet.ofRanges(Range.greaterThan(type, nativeValue.apply(Double.NEGATIVE_INFINITY))),
                        ValueSet.ofRanges(Range.lessThan(type, nativeValue.apply(Double.POSITIVE_INFINITY))));
                JdbcColumnHandle column = new JdbcColumnHandle("x", type.equals(DOUBLE) ? JDBC_DOUBLE : JDBC_REAL, type);
                JdbcNamedRelationHandle table = new JdbcNamedRelationHandle(new SchemaTableName("example", "numbers"), new RemoteTableName(Optional.empty(), Optional.empty(), "numbers"), Optional.empty());
                QueryBuilder builder = new FloatingPointQueryBuilder(RemoteQueryModifier.NONE);
                for (ValueSet valueSet : domains) {
                    for (boolean nullAllowed : List.of(false, true)) {
                        Domain domain = Domain.create(valueSet, nullAllowed);
                        DomainPushdownResult result = FLOATING_POINT_PUSHDOWN.apply(SESSION, domain);
                        assertThat(result.getRemainingFilter().isAll()).isTrue();
                        PreparedQuery query = builder.prepareSelectQuery(database.getJdbcClient(), SESSION, connection, table, Optional.empty(), List.of(column), Map.of(), TupleDomain.withColumnDomains(Map.of(column, result.getPushedDown())), Optional.empty());
                        List<Double> actual = new ArrayList<>();
                        try (PreparedStatement statement = builder.prepareStatement(database.getJdbcClient(), SESSION, connection, query, Optional.empty());
                                ResultSet rows = statement.executeQuery()) {
                            while (rows.next()) {
                                double value = rows.getDouble(1);
                                actual.add(rows.wasNull() ? null : value);
                            }
                        }
                        // SQL storage may normalize signed zero. Compare it by numeric value.
                        assertThat(actual.stream().map(value -> value == null ? null : value == 0 ? 0.0 : value).toList())
                                .as("%s", domain)
                                .containsExactlyInAnyOrderElementsOf(values.stream()
                                        .filter(value -> domain.includesNullableValue(value == null ? null : nativeValue.apply(value)))
                                        .map(value -> value == null ? null : value == 0 ? 0.0 : value)
                                        .toList());
                    }
                }
                for (boolean nullAllowed : List.of(false, true)) {
                    Domain nan = Domain.create(ValueSet.of(type, nativeValue.apply(Double.NaN)), nullAllowed);
                    DomainPushdownResult result = FLOATING_POINT_PUSHDOWN.apply(SESSION, nan);
                    assertThat(result.getRemainingFilter()).isEqualTo(nan);
                    assertThat(result.getPushedDown()).isEqualTo(Domain.create(ValueSet.all(type), nullAllowed));
                }
            }
        }
    }

    @Test
    void testNumberPredicates()
            throws SQLException
    {
        try (TestingDatabase database = new TestingDatabase()) {
            Connection connection = database.getConnection();
            connection.createStatement().execute("CREATE TABLE \"number_values\" (\"x\" DECIMAL(20, 4))");
            connection.createStatement().execute("INSERT INTO \"number_values\" VALUES (NULL), (-2), (-1), (0), (1), (2)");
            JdbcTypeHandle jdbcType = new JdbcTypeHandle(Types.DECIMAL, Optional.of("decimal"), Optional.of(20), Optional.of(4), Optional.empty(), Optional.empty());
            JdbcColumnHandle column = new JdbcColumnHandle("x", jdbcType, NUMBER);
            JdbcClient client = new ForwardingJdbcClient()
            {
                @Override
                protected JdbcClient delegate()
                {
                    return database.getJdbcClient();
                }

                @Override
                public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
                {
                    if (typeHandle.equals(jdbcType)) {
                        return Optional.of(numberColumnMapping());
                    }
                    return super.toColumnMapping(session, connection, typeHandle);
                }
            };
            TrinoNumber one = TrinoNumber.from(BigDecimal.ONE);
            TrinoNumber negativeOne = TrinoNumber.from(BigDecimal.ONE.negate());
            List<TrinoNumber> values = new ArrayList<>();
            values.add(null);
            for (long value : List.of(-2L, -1L, 0L, 1L, 2L)) {
                values.add(TrinoNumber.from(BigDecimal.valueOf(value)));
            }
            JdbcNamedRelationHandle table = new JdbcNamedRelationHandle(new SchemaTableName("example", "number_values"), new RemoteTableName(Optional.empty(), Optional.empty(), "number_values"), Optional.empty());
            QueryBuilder builder = new DefaultQueryBuilder(RemoteQueryModifier.NONE);
            PredicatePushdownController controller = numberColumnMapping().getPredicatePushdownController();
            for (ValueSet valueSet : List.of(
                    ValueSet.of(NUMBER, one),
                    ValueSet.of(NUMBER, negativeOne, one),
                    ValueSet.ofRanges(Range.range(NUMBER, negativeOne, true, one, true)))) {
                for (boolean nullAllowed : List.of(false, true)) {
                    Domain domain = Domain.create(valueSet, nullAllowed);
                    DomainPushdownResult result = controller.apply(SESSION, domain);
                    assertThat(result.getPushedDown()).isEqualTo(domain);
                    assertThat(result.getRemainingFilter().isAll()).isTrue();
                    PreparedQuery query = builder.prepareSelectQuery(client, SESSION, connection, table, Optional.empty(), List.of(column), Map.of(), TupleDomain.withColumnDomains(Map.of(column, result.getPushedDown())), Optional.empty());
                    List<TrinoNumber> actual = new ArrayList<>();
                    try (PreparedStatement statement = builder.prepareStatement(client, SESSION, connection, query, Optional.empty());
                            ResultSet rows = statement.executeQuery()) {
                        while (rows.next()) {
                            BigDecimal value = rows.getBigDecimal(1);
                            actual.add(value == null ? null : TrinoNumber.from(value));
                        }
                    }
                    assertThat(actual.stream().map(value -> value == null ? null : value.toBigDecimal()).toList())
                            .containsExactlyInAnyOrderElementsOf(values.stream()
                                    .filter(domain::includesNullableValue)
                                    .map(value -> value == null ? null : value.toBigDecimal())
                                    .toList());
                }
            }
            for (TrinoNumber value : List.of(
                    TrinoNumber.from(new TrinoNumber.NotANumber()),
                    TrinoNumber.from(new TrinoNumber.Infinity(false)),
                    TrinoNumber.from(new TrinoNumber.Infinity(true)))) {
                Domain domain = Domain.singleValue(NUMBER, value);
                DomainPushdownResult result = controller.apply(SESSION, domain);
                assertThat(result.getPushedDown()).isEqualTo(Domain.notNull(NUMBER));
                assertThat(result.getRemainingFilter()).isEqualTo(domain);
            }
            Domain unbounded = Domain.create(ValueSet.ofRanges(Range.greaterThan(NUMBER, one)), false);
            DomainPushdownResult result = controller.apply(SESSION, unbounded);
            assertThat(result.getPushedDown()).isEqualTo(unbounded);
            assertThat(result.getRemainingFilter()).isEqualTo(unbounded);
        }
    }

    @Test
    void testFiniteStoragePushdown()
    {
        for (Type type : List.of(DOUBLE, REAL)) {
            DoubleFunction<Object> nativeValue = type.equals(DOUBLE) ? value -> value : value -> (long) floatToRawIntBits((float) value);
            for (boolean nullAllowed : List.of(false, true)) {
                for (ValueSet values : List.of(
                        ValueSet.ofRanges(Range.lessThan(type, nativeValue.apply(0))),
                        ValueSet.ofRanges(Range.greaterThan(type, nativeValue.apply(0))),
                        ValueSet.of(type, nativeValue.apply(Double.NaN)).complement())) {
                    Domain domain = Domain.create(values, nullAllowed);
                    DomainPushdownResult result = FINITE_FLOATING_POINT_PUSHDOWN.apply(SESSION, domain);
                    assertThat(result.getRemainingFilter().isAll()).isTrue();
                    for (Double value : List.of(-Double.MAX_VALUE, -1.0, 0.0, 1.0, Double.MAX_VALUE)) {
                        if (type.equals(REAL) && !Float.isFinite(value.floatValue())) {
                            continue;
                        }
                        assertThat(result.getPushedDown().includesNullableValue(nativeValue.apply(value))).isEqualTo(domain.includesNullableValue(nativeValue.apply(value)));
                    }
                    assertThat(result.getPushedDown().isNullAllowed()).isEqualTo(nullAllowed);
                }
                Domain infinity = Domain.create(ValueSet.of(type, nativeValue.apply(Double.POSITIVE_INFINITY)), nullAllowed);
                DomainPushdownResult result = FINITE_FLOATING_POINT_PUSHDOWN.apply(SESSION, infinity);
                assertThat(result.getRemainingFilter()).isEqualTo(infinity);
                assertThat(result.getPushedDown()).isEqualTo(Domain.create(ValueSet.all(type), nullAllowed));
            }
        }
    }
}
