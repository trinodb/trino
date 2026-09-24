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
package io.trino.plugin.thrift;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.thrift.util.TupleDomainConversion.tupleDomainToThriftTupleDomain;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestThriftMetadata
{
    @Test
    public void testNaNFilterRetainsResidualAndIndexIsDeclined()
    {
        ThriftMetadata metadata = new ThriftMetadata(
                (_, _) -> { throw new AssertionError("Unexpected remote call"); },
                _ -> Map.of(),
                TESTING_TYPE_MANAGER,
                Runnable::run);
        ThriftColumnHandle column = new ThriftColumnHandle("x", DOUBLE, Optional.empty(), false);
        ThriftTableHandle table = new ThriftTableHandle("schema", "table", TupleDomain.all(), Optional.empty());
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(Map.of(column, Domain.create(ValueSet.of(DOUBLE, 1.0, Double.NaN), false)));
        var result = metadata.applyFilter(SESSION, table, new Constraint(constraint)).orElseThrow();
        assertThat(result.getRemainingFilter()).isEqualTo(constraint);
        ThriftTableHandle filtered = (ThriftTableHandle) result.getHandle();
        assertThat(filtered.constraint()).isEqualTo(TupleDomain.withColumnDomains(Map.of(column, Domain.notNull(DOUBLE))));
        assertThat(tupleDomainToThriftTupleDomain(filtered.constraint())).isNotNull();
        assertThat(metadata.resolveIndex(SESSION, table, Set.of(column), Set.of(column), constraint)).isEmpty();
    }
}
