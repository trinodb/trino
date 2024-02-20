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
package io.trino.plugin.raptor.legacy.metadata;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class Distribution
{
    private static final JsonCodec<List<String>> LIST_CODEC = listJsonCodec(String.class);

    private final long id;
    private final Optional<String> name;
    private final List<Type> columnTypes;
    private final int bucketCount;

    public Distribution(long id, Optional<String> name, List<Type> columnTypes, int bucketCount)
    {
        this.id = id;
        this.name = requireNonNull(name, "name is null");
        this.columnTypes = ImmutableList.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        this.bucketCount = bucketCount;
    }

    public long getId()
    {
        return id;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    public int getBucketCount()
    {
        return bucketCount;
    }

    public static class Mapper
            implements RowMapper<Distribution>
    {
        private final TypeManager typeManager;

        @Inject
        public Mapper(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public Distribution map(ResultSet rs, StatementContext ctx)
                throws SQLException
        {
            List<Type> types = LIST_CODEC.fromJson(rs.getString("column_types")).stream()
                    .map(TypeId::of)
                    .map(typeManager::getType)
                    .collect(toImmutableList());

            return new Distribution(
                    rs.getLong("distribution_id"),
                    Optional.ofNullable(rs.getString("distribution_name")),
                    types,
                    rs.getInt("bucket_count"));
        }
    }

    public static String serializeColumnTypes(List<Type> columnTypes)
    {
        return LIST_CODEC.toJson(columnTypes.stream()
                .map(type -> type.getTypeId().getId())
                .collect(toList()));
    }
}
