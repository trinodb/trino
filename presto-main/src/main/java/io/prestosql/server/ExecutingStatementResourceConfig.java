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
package io.prestosql.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ExecutingStatementResourceConfig
{
    private Optional<DataSize> serverTargetResultSize;

    public Optional<DataSize> getServerTargetResultSize()
    {
        return serverTargetResultSize;
    }

    @Config("query.target-result-size.default-mb")
    @ConfigDescription("Chunk size (in MB) for query results in JSON responses sent to client")
    public ExecutingStatementResourceConfig setServerTargetResultSize(@Nullable String targetResultSize)
    {
        this.serverTargetResultSize = Optional.of(new DataSize(Integer.parseInt(targetResultSize), MEGABYTE));
        return this;
    }
}
