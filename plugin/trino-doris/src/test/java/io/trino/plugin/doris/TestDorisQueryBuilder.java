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
package io.trino.plugin.doris;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDorisQueryBuilder
{
    @Test
    void testBuildSplitPlanningSql()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();

        assertThat(queryBuilder.buildSplitPlanningSql(new DorisTableHandle("sales", "orders")))
                .isEqualTo("SELECT * FROM `sales`.`orders`");
    }

    @Test
    void testBuildSqlUsesRemoteNames()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        DorisTableHandle tableHandle = new DorisTableHandle("sales", "orders", "Sales", "Orders");

        assertThat(queryBuilder.buildSplitPlanningSql(tableHandle))
                .isEqualTo("SELECT * FROM `Sales`.`Orders`");
        assertThat(queryBuilder.buildSelectSql(tableHandle, List.of("id"), List.of(11L)))
                .isEqualTo("SELECT `id` FROM `Sales`.`Orders` TABLET(11)");
    }

    @Test
    void testBuildSelectSqlUsesLiteralForEmptyProjection()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();

        assertThat(queryBuilder.buildSelectSql(
                new DorisTableHandle("sales", "orders"),
                List.of(),
                List.of()))
                .isEqualTo("SELECT 1 FROM `sales`.`orders`");
    }

    @Test
    void testBuildSelectSqlWithProjectionAndTablets()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();

        assertThat(queryBuilder.buildSelectSql(
                new DorisTableHandle("sales", "orders"),
                List.of("id", "event`time"),
                List.of(11L, 12L)))
                .isEqualTo("SELECT `id`, `event``time` FROM `sales`.`orders` TABLET(11,12)");
    }

    @Test
    void testBuildSelectSqlRejectsNullColumnName()
    {
        DorisQueryBuilder queryBuilder = new DorisQueryBuilder();
        List<String> columnNames = new ArrayList<>();
        columnNames.add("id");
        columnNames.add(null);

        assertThatThrownBy(() -> queryBuilder.buildSelectSql(
                new DorisTableHandle("sales", "orders"),
                columnNames,
                List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("identifier is null");
    }
}
