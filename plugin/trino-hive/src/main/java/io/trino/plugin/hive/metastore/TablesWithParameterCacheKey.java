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
package io.trino.plugin.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import java.util.Objects;

@Immutable
public class TablesWithParameterCacheKey
{
    private final String databaseName;
    private final String parameterKey;
    private final String parameterValue;

    @JsonCreator
    public TablesWithParameterCacheKey(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("parameterKey") String parameterKey,
            @JsonProperty("parameterValue") String parameterValue)
    {
        this.databaseName = databaseName;
        this.parameterKey = parameterKey;
        this.parameterValue = parameterValue;
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getParameterKey()
    {
        return parameterKey;
    }

    @JsonProperty
    public String getParameterValue()
    {
        return parameterValue;
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

        TablesWithParameterCacheKey other = (TablesWithParameterCacheKey) o;
        return Objects.equals(databaseName, other.databaseName) &&
                Objects.equals(parameterKey, other.parameterKey) &&
                Objects.equals(parameterValue, other.parameterValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, parameterKey, parameterValue);
    }
}
