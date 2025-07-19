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
package io.trino.server.protocol;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

public class ExecutingStatementResourceConfig
{
    static final String MIN_TARGET_RESULT_SIZE = "1MB";
    static final String MAX_TARGET_RESULT_SIZE = "1024MB";
    private DataSize targetResultSize = DataSize.valueOf(MIN_TARGET_RESULT_SIZE);

    @MinDataSize(MIN_TARGET_RESULT_SIZE)
    @MaxDataSize(MAX_TARGET_RESULT_SIZE)
    public DataSize getTargetResultSize()
    {
        return targetResultSize;
    }

    @Config("query.target-result-size")
    @ConfigDescription("Target result size for a single response for query results streamed from coordinator to client")
    public ExecutingStatementResourceConfig setTargetResultSize(DataSize targetResultSize)
    {
        this.targetResultSize = targetResultSize;
        return this;
    }
}
