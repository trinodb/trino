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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestingJdbcUtils
{
    private TestingJdbcUtils() {}

    public static List<List<Object>> readRows(ResultSet rs)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        int columnCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }
        return rows.build();
    }

    public static List<List<Object>> readRows(ResultSet rs, List<String> columns)
            throws SQLException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (String column : columns) {
                row.add(rs.getObject(column));
            }
            rows.add(row);
        }
        return rows.build();
    }

    @SafeVarargs
    public static <T> List<T> list(T... elements)
    {
        return asList(elements);
    }

    @SafeVarargs
    public static <T> T[] array(T... elements)
    {
        return elements;
    }

    public static ResultSetAssert assertResultSet(ResultSet resultSet)
    {
        return new ResultSetAssert(resultSet);
    }

    public static class ResultSetAssert
    {
        private final ResultSet resultSet;

        public ResultSetAssert(ResultSet resultSet)
        {
            this.resultSet = requireNonNull(resultSet, "resultSet is null");
        }

        public ResultSetAssert hasColumnCount(int expectedColumnCount)
                throws SQLException
        {
            assertThat(resultSet.getMetaData().getColumnCount()).isEqualTo(expectedColumnCount);
            return this;
        }

        public ResultSetAssert hasColumn(int columnIndex, String name, int sqlType)
                throws SQLException
        {
            assertThat(resultSet.getMetaData().getColumnName(columnIndex)).isEqualTo(name);
            assertThat(resultSet.getMetaData().getColumnType(columnIndex)).isEqualTo(sqlType);
            return this;
        }

        public ResultSetAssert hasRows(List<List<?>> expected)
                throws SQLException
        {
            assertThat(readRows(resultSet)).isEqualTo(expected);
            return this;
        }
    }
}
