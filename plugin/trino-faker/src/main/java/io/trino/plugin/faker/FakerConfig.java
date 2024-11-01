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

package io.trino.plugin.faker;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class FakerConfig
{
    private double nullProbability = 0.5;
    private long defaultLimit = 1000L;

    @Max(1)
    @Min(0)
    public double getNullProbability()
    {
        return nullProbability;
    }

    @Config("faker.null-probability")
    @ConfigDescription("Default null probability for any column in any table that allows them")
    public FakerConfig setNullProbability(double value)
    {
        this.nullProbability = value;
        return this;
    }

    @Min(1)
    public long getDefaultLimit()
    {
        return defaultLimit;
    }

    @Config("faker.default-limit")
    @ConfigDescription("Default number of rows for each table, when the LIMIT clause is not specified in the query")
    public FakerConfig setDefaultLimit(long value)
    {
        this.defaultLimit = value;
        return this;
    }
}
