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
package io.trino.spi.connector;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public final class MaterializedViewFreshness
{
    private final boolean materializedViewFresh;
    private final Optional<Instant> lastRefreshTime;

    @Deprecated
    public MaterializedViewFreshness(boolean materializedViewFresh)
    {
        this(materializedViewFresh, Optional.empty());
    }

    public MaterializedViewFreshness(boolean materializedViewFresh, Optional<Instant> lastRefreshTime)
    {
        this.materializedViewFresh = materializedViewFresh;
        this.lastRefreshTime = requireNonNull(lastRefreshTime, "lastRefreshTime is null");
    }

    public boolean isMaterializedViewFresh()
    {
        return materializedViewFresh;
    }

    /**
     * Last materialized view's refresh time, if known.
     */
    public Optional<Instant> getLastRefreshTime()
    {
        return lastRefreshTime;
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
        MaterializedViewFreshness that = (MaterializedViewFreshness) o;
        return materializedViewFresh == that.materializedViewFresh &&
                Objects.equals(lastRefreshTime, that.lastRefreshTime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(materializedViewFresh, lastRefreshTime);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", MaterializedViewFreshness.class.getSimpleName() + "[", "]")
                .add("materializedViewFresh=" + materializedViewFresh)
                .add("lastRefreshTime=" + lastRefreshTime)
                .toString();
    }
}
