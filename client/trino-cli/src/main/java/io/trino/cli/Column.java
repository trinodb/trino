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
package io.trino.cli;

import com.google.common.collect.ImmutableSet;

import java.sql.Types;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class Column
{
    private static final Set<Integer> NUMERIC_TYPES = ImmutableSet.of(
            Types.TINYINT,
            Types.SMALLINT,
            Types.INTEGER,
            Types.BIGINT,
            Types.REAL,
            Types.FLOAT,
            Types.DOUBLE,
            Types.DECIMAL,
            Types.NUMERIC);

    private final String name;
    private final int jdbcType;

    /**
     * @param jdbcType one of the {@link Types} constants
     */
    public Column(String name, int jdbcType)
    {
        this.name = requireNonNull(name, "name is null");
        this.jdbcType = jdbcType;
    }

    public String getName()
    {
        return name;
    }

    public int getJdbcType()
    {
        return jdbcType;
    }

    public boolean isNumeric()
    {
        return NUMERIC_TYPES.contains(jdbcType);
    }

    @Override
    public String toString()
    {
        return name;
    }
}
