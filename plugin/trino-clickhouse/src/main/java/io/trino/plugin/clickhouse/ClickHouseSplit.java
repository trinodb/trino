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
package io.trino.plugin.clickhouse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.HostAddress;

import java.util.List;
import java.util.Optional;

public class ClickHouseSplit
        extends JdbcSplit
{
    private final List<HostAddress> addresses;
    private final Optional<String> tableName;

    @JsonCreator
    public ClickHouseSplit(
            @JsonProperty("additionalPredicate") Optional<String> additionalPredicate,
            @JsonProperty("targetAddress") List<HostAddress> addresses,
            @JsonProperty("tableName") Optional<String> tableName)
    {
        super(additionalPredicate);
        this.addresses = addresses;
        this.tableName = tableName;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return this.addresses;
    }

    @JsonProperty
    public Optional<String> getTableName()
    {
        return this.tableName;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
