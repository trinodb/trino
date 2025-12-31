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
    private final Freshness freshness;
    private final Optional<Instant> lastKnownFreshTime;

    public MaterializedViewFreshness(Freshness freshness, Optional<Instant> lastKnownFreshTime)
    {
        this.freshness = requireNonNull(freshness, "freshness is null");
        this.lastKnownFreshTime = requireNonNull(lastKnownFreshTime, "lastKnownFreshTime is null");
    }

    public Freshness getFreshness()
    {
        return freshness;
    }

    /**
     * Last time when the materialized view was known to be fresh.
     */
    public Optional<Instant> getLastKnownFreshTime()
    {
        return lastKnownFreshTime;
    }

    /**
     * @deprecated Use {@link #getLastKnownFreshTime()} instead.
     */
    @Deprecated
    public Optional<Instant> getLastFreshTime()
    {
        return getLastKnownFreshTime();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedViewFreshness that = (MaterializedViewFreshness) obj;
        return freshness == that.freshness
                && Objects.equals(lastKnownFreshTime, that.lastKnownFreshTime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(freshness, lastKnownFreshTime);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", MaterializedViewFreshness.class.getSimpleName() + "[", "]")
                .add("freshness=" + freshness)
                .add("lastKnownFreshTime=" + lastKnownFreshTime.orElse(null))
                .toString();
    }

    public enum Freshness
    {
        FRESH,
        /**
         * The materialized view is fresh enough within its grace period. Returned when a connector
         * is invoked with {@code considerGracePeriod = true} and determined it could skip expensive
         * freshness checks (e.g., checking base table snapshots) because the MV is still within
         * the grace period bounds. The actual freshness of base tables was not verified, but worst-case
         * staleness is guaranteed to be within the acceptable grace period.
         */
        FRESH_WITHIN_GRACE_PERIOD,
        STALE,
        UNKNOWN,
        /**/
    }
}
