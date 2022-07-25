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
package io.trino.plugin.deltalake.transactionlog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;

public class TransactionEntry
{
    private final String appId;
    private final long version;
    private final long lastUpdated;

    @JsonCreator
    public TransactionEntry(
            @JsonProperty("appId") String appId,
            @JsonProperty("version") long version,
            @JsonProperty("lastUpdated") long lastUpdated)
    {
        this.appId = appId;
        this.version = version;
        this.lastUpdated = lastUpdated;
    }

    @JsonProperty
    public String getAppId()
    {
        return appId;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public long getLastUpdated()
    {
        return lastUpdated;
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
        TransactionEntry that = (TransactionEntry) o;
        return version == that.version &&
                lastUpdated == that.lastUpdated &&
                Objects.equals(appId, that.appId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(appId, version, lastUpdated);
    }

    @Override
    public String toString()
    {
        return format("TransactionEntry{appId=%s, version=%d, lastUpdated=%d}", appId, version, lastUpdated);
    }
}
