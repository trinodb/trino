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
package io.prestosql.plugin.redshift;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestRedshiftClient
{
    @Test
    public void testGenerateCreateTableSqlWithCompoundSortKey()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("col_one integer");
        builder.add("col_two varchar");
        ImmutableList<String> colList = builder.build();

        Optional<String> distKey = Optional.empty();
        Optional<String> compoundSortKey = Optional.of("col_one,col_two");
        Optional<String> interleavedSortKey = Optional.empty();

        String sql = RedshiftClient.generateCreateTableSql("schema.table_name", colList, distKey, compoundSortKey, interleavedSortKey);

        assertEquals(sql, "CREATE TABLE schema.table_name (col_one integer, col_two varchar) compound sortkey(col_one,col_two) ");
    }

    @Test
    public void testGenerateCreateTableSqlWithInterleavedSortKey()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("col_one integer");
        builder.add("col_two varchar");
        ImmutableList<String> colList = builder.build();

        Optional<String> distKey = Optional.empty();
        Optional<String> compoundSortKey = Optional.empty();
        Optional<String> interleavedSortKey = Optional.of("col_one,col_two");

        String sql = RedshiftClient.generateCreateTableSql("schema.table_name", colList, distKey, compoundSortKey, interleavedSortKey);

        assertEquals(sql, "CREATE TABLE schema.table_name (col_one integer, col_two varchar) interleaved sortkey(col_one,col_two) ");
    }

    @Test
    public void testGenerateCreateTableSqlWithDistKey()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("col_one integer");
        builder.add("col_two varchar");
        ImmutableList<String> colList = builder.build();

        Optional<String> distKey = Optional.of("col_one");
        Optional<String> compoundSortKey = Optional.empty();
        Optional<String> interleavedSortKey = Optional.empty();

        String sql = RedshiftClient.generateCreateTableSql("schema.table_name", colList, distKey, compoundSortKey, interleavedSortKey);

        assertEquals(sql, "CREATE TABLE schema.table_name (col_one integer, col_two varchar) distkey(col_one) ");
    }

    @Test
    public void testGenerateCreateTableSqlWithDistKeyAndSortKey()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("col_one integer");
        builder.add("col_two varchar");
        ImmutableList<String> colList = builder.build();

        Optional<String> distKey = Optional.of("col_one");
        Optional<String> compoundSortKey = Optional.empty();
        Optional<String> interleavedSortKey = Optional.of("col_one,col_two");

        String sql = RedshiftClient.generateCreateTableSql("schema.table_name", colList, distKey, compoundSortKey, interleavedSortKey);

        assertEquals(sql, "CREATE TABLE schema.table_name (col_one integer, col_two varchar) distkey(col_one) interleaved sortkey(col_one,col_two) ");
    }

    @Test
    public void testGenerateCreateTableSqlWithBothCompoundAndInterleavedSortKeyFails()
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        builder.add("col_one integer");
        builder.add("col_two varchar");
        ImmutableList<String> colList = builder.build();

        Optional<String> distKey = Optional.empty();
        Optional<String> compoundSortKey = Optional.of("col_one");
        Optional<String> interleavedSortKey = Optional.of("col_one");

        assertThrows(IllegalArgumentException.class, () -> RedshiftClient.generateCreateTableSql("schema.table_name", colList, distKey, compoundSortKey, interleavedSortKey));
    }
}
