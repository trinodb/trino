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
package io.trino.plugin.jdbc;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RemoteDatabaseEvent
{
    private final String query;
    private final Status status;

    public RemoteDatabaseEvent(String query, Status status)
    {
        this.query = requireNonNull(query, "query is null");
        this.status = requireNonNull(status, "status is null");
    }

    public String getQuery()
    {
        return query;
    }

    public Status getStatus()
    {
        return status;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteDatabaseEvent that = (RemoteDatabaseEvent) o;
        return query.equals(that.query) && status == that.status;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, status);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("query", query)
                .add("status", status)
                .toString();
    }

    public enum Status
    {
        RUNNING,
        CANCELLED,
        DONE,
    }
}
