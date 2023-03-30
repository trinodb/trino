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
package io.trino.plugin.tpcds;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;

public class TpcdsConfig
{
    private int splitsPerNode = Runtime.getRuntime().availableProcessors();
    private boolean withNoSexism;
    private Integer splitCount;

    @Min(1)
    public int getSplitsPerNode()
    {
        return splitsPerNode;
    }

    @Config("tpcds.splits-per-node")
    public TpcdsConfig setSplitsPerNode(int splitsPerNode)
    {
        this.splitsPerNode = splitsPerNode;
        return this;
    }

    public boolean isWithNoSexism()
    {
        return withNoSexism;
    }

    @Config("tpcds.with-no-sexism")
    public TpcdsConfig setWithNoSexism(boolean withNoSexism)
    {
        this.withNoSexism = withNoSexism;
        return this;
    }

    @Min(1)
    public Integer getSplitCount()
    {
        return splitCount;
    }

    @Config("tpcds.split-count")
    @ConfigDescription("Number of split to be created. If not specified the number of splits is computed as 'tpcds.splits-per-node * <number of active nodes>'")
    public TpcdsConfig setSplitCount(Integer splitCount)
    {
        this.splitCount = splitCount;
        return this;
    }
}
