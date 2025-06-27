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
package io.trino.plugin.cassandra.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.cassandra.CassandraClusteringPredicatesExtractor;
import io.trino.plugin.cassandra.CassandraColumnHandle;
import io.trino.plugin.cassandra.CassandraNamedRelationHandle;
import io.trino.plugin.cassandra.CassandraTable;
import io.trino.plugin.cassandra.CassandraTypes;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.cassandra.CassandraTestingUtils.CASSANDRA_TYPE_MANAGER;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCassandraClusteringPredicatesExtractor
{
    private static final CassandraColumnHandle col1;
    private static final CassandraColumnHandle col2;
    private static final CassandraColumnHandle col3;
    private static final CassandraColumnHandle col4;
    private static final CassandraTable cassandraTable;

    static {
        col1 = new CassandraColumnHandle("partitionKey1", 1, CassandraTypes.BIGINT, true, false, false, false);
        col2 = new CassandraColumnHandle("clusteringKey1", 2, CassandraTypes.BIGINT, false, true, false, false);
        col3 = new CassandraColumnHandle("clusteringKey2", 3, CassandraTypes.BIGINT, false, true, false, false);
        col4 = new CassandraColumnHandle("clusteringKe3", 4, CassandraTypes.BIGINT, false, true, false, false);

        cassandraTable = new CassandraTable(
                new CassandraNamedRelationHandle("test", "records"), ImmutableList.of(col1, col2, col3, col4));
    }

    @Test
    public void testBuildClusteringPredicate()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        col1, Domain.singleValue(BIGINT, 23L),
                        col2, Domain.singleValue(BIGINT, 34L),
                        col4, Domain.singleValue(BIGINT, 26L)));
        CassandraClusteringPredicatesExtractor predicatesExtractor = new CassandraClusteringPredicatesExtractor(CASSANDRA_TYPE_MANAGER, cassandraTable.clusteringKeyColumns(), tupleDomain);
        String predicate = predicatesExtractor.getClusteringKeyPredicates();
        assertThat(predicate).isEqualTo("\"clusteringKey1\" = 34");
    }

    @Test
    public void testGetUnenforcedPredicates()
    {
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        col2, Domain.singleValue(BIGINT, 34L),
                        col4, Domain.singleValue(BIGINT, 26L)));
        CassandraClusteringPredicatesExtractor predicatesExtractor = new CassandraClusteringPredicatesExtractor(CASSANDRA_TYPE_MANAGER, cassandraTable.clusteringKeyColumns(), tupleDomain);
        TupleDomain<ColumnHandle> unenforcedPredicates = TupleDomain.withColumnDomains(ImmutableMap.of(col4, Domain.singleValue(BIGINT, 26L)));
        assertThat(predicatesExtractor.getUnenforcedConstraints()).isEqualTo(unenforcedPredicates);
    }
}
