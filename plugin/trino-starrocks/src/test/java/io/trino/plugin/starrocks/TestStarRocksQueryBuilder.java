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
package io.trino.plugin.starrocks;

import io.trino.spi.predicate.Domain;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.predicate.ValueSet.ofRanges;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestStarRocksQueryBuilder
{
    @Test
    void testBuildSqlUsesRemoteNames()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();
        StarRocksTableHandle tableHandle = new StarRocksTableHandle("sales", "orders", Optional.empty(), "Sales", "Orders", StarRocksRelationType.TABLE);

        assertThat(queryBuilder.buildSelectSql(
                tableHandle,
                List.of(
                        new StarRocksColumnHandle("orderkey", "OrderKey", BIGINT, 0),
                        new StarRocksColumnHandle("customer_name", "Customer``Name", createVarcharType(20), 1))))
                .isEqualTo("SELECT `OrderKey` AS `orderkey`, `Customer````Name` AS `customer_name` FROM `Sales`.`Orders`");
    }

    @Test
    void testBuildSelectSqlWithEmptyProjection()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();

        assertThat(queryBuilder.buildSelectSql(
                new StarRocksTableHandle("sales", "orders", Optional.empty(), "sales", "orders", StarRocksRelationType.TABLE),
                List.of()))
                .isEqualTo("SELECT 1 FROM `sales`.`orders`");
    }

    @Test
    void testBuildSelectSqlWithCatalogPredicateSortAndLimit()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();
        StarRocksColumnHandle orderKey = new StarRocksColumnHandle("orderkey", "OrderKey", BIGINT, 0);
        StarRocksTableHandle tableHandle = new StarRocksTableHandle("sales", "orders", Optional.of("external_catalog"), "Sales", "Orders", StarRocksRelationType.TABLE)
                .withConstraint(withColumnDomains(Map.of(orderKey, Domain.create(ofRanges(range(BIGINT, 10L, true, 20L, false)), false))))
                .withTopN(5, List.of(new StarRocksSortItem("orderkey", "OrderKey", ASC_NULLS_LAST)));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of(orderKey)))
                .isEqualTo("SELECT `OrderKey` AS `orderkey` FROM `external_catalog`.`Sales`.`Orders` WHERE (`OrderKey` >= 10 AND `OrderKey` < 20) ORDER BY `OrderKey` IS NULL ASC, `OrderKey` ASC LIMIT 5");
    }

    @Test
    void testBuildSelectSqlWithAggregation()
    {
        StarRocksQueryBuilder queryBuilder = new StarRocksQueryBuilder();
        StarRocksColumnHandle aggregate = new StarRocksColumnHandle("_starrocks_agg_0", "_starrocks_agg_0", BIGINT, 0);
        StarRocksTableHandle tableHandle = new StarRocksTableHandle("sales", "orders", Optional.empty(), "sales", "orders", StarRocksRelationType.TABLE)
                .withAggregation(new StarRocksAggregation(
                        List.of(),
                        List.of(new StarRocksAggregateColumn("_starrocks_agg_0", "count(*)", BIGINT))));

        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of(aggregate)))
                .isEqualTo("SELECT count(*) AS `_starrocks_agg_0` FROM `sales`.`orders`");
    }

    @Test
    void testUnsupportedFloatingPointPredicateLiteralIsNotPushedDown()
    {
        StarRocksColumnHandle ratio = new StarRocksColumnHandle("ratio", "ratio", DOUBLE, 0);

        assertThat(StarRocksQueryBuilder.buildColumnPredicate(ratio, Domain.singleValue(DOUBLE, Double.POSITIVE_INFINITY)))
                .isEmpty();
    }
}
