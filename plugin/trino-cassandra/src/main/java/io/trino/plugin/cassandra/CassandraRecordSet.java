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
package io.trino.plugin.cassandra;

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CassandraRecordSet
        implements RecordSet
{
    private final CassandraSession cassandraSession;
    private final CassandraTypeManager cassandraTypeManager;
    private final String cql;
    private final List<String> cassandraNames;
    private final List<CassandraType> cassandraTypes;
    private final List<Type> columnTypes;

    public CassandraRecordSet(CassandraSession cassandraSession, CassandraTypeManager cassandraTypeManager, String cql, List<CassandraColumnHandle> cassandraColumns)
    {
        this.cassandraSession = requireNonNull(cassandraSession, "cassandraSession is null");
        this.cassandraTypeManager = requireNonNull(cassandraTypeManager, "cassandraTypeManager is null");
        this.cql = requireNonNull(cql, "cql is null");

        requireNonNull(cassandraColumns, "cassandraColumns is null");
        this.cassandraNames = transformList(cassandraColumns, CassandraColumnHandle::getName);
        this.cassandraTypes = transformList(cassandraColumns, CassandraColumnHandle::getCassandraType);
        this.columnTypes = transformList(cassandraColumns, CassandraColumnHandle::getType);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new CassandraRecordCursor(cassandraSession, cassandraTypeManager, cassandraNames, cassandraTypes, cql);
    }

    private static <T, R> List<R> transformList(List<T> list, Function<T, R> function)
    {
        return list.stream().map(function).collect(toImmutableList());
    }
}
