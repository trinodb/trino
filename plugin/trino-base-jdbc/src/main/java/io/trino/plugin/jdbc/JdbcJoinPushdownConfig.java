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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import java.util.Optional;

public class JdbcJoinPushdownConfig
{
    private JoinPushdownStrategy joinPushdownStrategy = JoinPushdownStrategy.AUTOMATIC;
    private Optional<DataSize> joinPushdownAutomaticMaxTableSize = Optional.empty();
    // Normally we would put 1.0 as a default value here to only allow joins which do not expand data to be pushed down.
    // We use 1.25 to adjust for the fact that NDV estimations sometimes are off and joins which should be pushed down are not.
    private double joinPushdownAutomaticMaxJoinToTablesRatio = 1.25;

    public JoinPushdownStrategy getJoinPushdownStrategy()
    {
        return joinPushdownStrategy;
    }

    @Config("join-pushdown.strategy")
    public JdbcJoinPushdownConfig setJoinPushdownStrategy(JoinPushdownStrategy joinPushdownStrategy)
    {
        this.joinPushdownStrategy = joinPushdownStrategy;
        return this;
    }

    public Optional<DataSize> getJoinPushdownAutomaticMaxTableSize()
    {
        return joinPushdownAutomaticMaxTableSize;
    }

    @Config("experimental.join-pushdown.automatic.max-table-size")
    @ConfigDescription("Maximum table size to be considered for join pushdown")
    public JdbcJoinPushdownConfig setJoinPushdownAutomaticMaxTableSize(DataSize joinPushdownAutomaticMaxTableSize)
    {
        this.joinPushdownAutomaticMaxTableSize = Optional.ofNullable(joinPushdownAutomaticMaxTableSize);
        return this;
    }

    public double getJoinPushdownAutomaticMaxJoinToTablesRatio()
    {
        return joinPushdownAutomaticMaxJoinToTablesRatio;
    }

    @Config("experimental.join-pushdown.automatic.max-join-to-tables-ratio")
    @ConfigDescription("If estimated join output size is greater than or equal to ratio * sum of table sizes, then join pushdown will not be performed")
    public JdbcJoinPushdownConfig setJoinPushdownAutomaticMaxJoinToTablesRatio(double joinPushdownAutomaticMaxJoinToTablesRatio)
    {
        this.joinPushdownAutomaticMaxJoinToTablesRatio = joinPushdownAutomaticMaxJoinToTablesRatio;
        return this;
    }
}
