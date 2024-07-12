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
package io.trino.plugin.atop;

import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.Domain;

import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.util.Objects.requireNonNull;

public record AtopTableHandle(
        String schema,
        AtopTable table,
        Domain startTimeConstraint,
        Domain endTimeConstraint)
        implements ConnectorTableHandle
{
    public AtopTableHandle(String schema, AtopTable table)
    {
        this(schema, table, Domain.all(TIMESTAMP_TZ_MILLIS), Domain.all(TIMESTAMP_TZ_MILLIS));
    }

    public AtopTableHandle
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");
        requireNonNull(startTimeConstraint, "startTimeConstraint is null");
        requireNonNull(endTimeConstraint, "endTimeConstraint is null");
    }
    @Override
    public String toString()
    {
        return schema + ":" + table;
    }
}
