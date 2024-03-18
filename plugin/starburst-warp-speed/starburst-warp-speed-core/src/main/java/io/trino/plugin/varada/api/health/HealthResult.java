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
package io.trino.plugin.varada.api.health;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.Objects;

public class HealthResult
{
    private final boolean ready;
    private final URI httpUri;
    private final long createEpochTime;
    private final ImmutableList<HealthNode> healthNodes;
    private final long totalCapacityMB;

    public HealthResult(boolean ready, URI httpUri, long createEpochTime, long totalCapacityMB)
    {
        this(ready, httpUri, createEpochTime, ImmutableList.of(), totalCapacityMB);
    }

    @JsonCreator
    public HealthResult(@JsonProperty("ready") boolean ready,
            @JsonProperty("httpUri") URI httpUri,
            @JsonProperty("createEpochTime") long createEpochTime,
            @JsonProperty("healthNodes") ImmutableList<HealthNode> healthNodes,
            @JsonProperty("totalCapacityMB") long totalCapacityMB)
    {
        this.ready = ready;
        this.httpUri = httpUri;
        this.createEpochTime = createEpochTime;
        this.healthNodes = healthNodes;
        this.totalCapacityMB = totalCapacityMB;
    }

    @JsonProperty("ready")
    public boolean isReady()
    {
        return ready;
    }

    @JsonProperty("httpUri")
    public URI getHttpUri()
    {
        return httpUri;
    }

    @JsonProperty("createEpochTime")
    public long getCreateEpochTime()
    {
        return createEpochTime;
    }

    @JsonProperty("healthNodes")
    public ImmutableList<HealthNode> getHealthNodes()
    {
        return healthNodes;
    }

    @JsonProperty("totalCapacityMB")
    public long getTotalCapacityMB()
    {
        return totalCapacityMB;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HealthResult that)) {
            return false;
        }
        return isReady() == that.isReady() && getCreateEpochTime() == that.getCreateEpochTime() && getTotalCapacityMB() == that.getTotalCapacityMB() && Objects.equals(getHttpUri(), that.getHttpUri()) && Objects.equals(getHealthNodes(), that.getHealthNodes());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(isReady(), getHttpUri(), getCreateEpochTime(), getHealthNodes(), getTotalCapacityMB());
    }

    @Override
    public String toString()
    {
        return "HealthResult{" +
                "ready=" + ready +
                ", httpUri=" + httpUri +
                ", createEpochTime=" + createEpochTime +
                ", healthNodes=" + healthNodes +
                ", totalCapacityMB=" + totalCapacityMB +
                '}';
    }
}
