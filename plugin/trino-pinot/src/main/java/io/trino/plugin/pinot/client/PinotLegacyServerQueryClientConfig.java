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
package io.trino.plugin.pinot.client;

import io.airlift.configuration.Config;

public class PinotLegacyServerQueryClientConfig
{
    private int maxRowsPerSplitForSegmentQueries = 50_000;

    public int getMaxRowsPerSplitForSegmentQueries()
    {
        return maxRowsPerSplitForSegmentQueries;
    }

    @Config("pinot.max-rows-per-split-for-segment-queries")
    public PinotLegacyServerQueryClientConfig setMaxRowsPerSplitForSegmentQueries(int maxRowsPerSplitForSegmentQueries)
    {
        this.maxRowsPerSplitForSegmentQueries = maxRowsPerSplitForSegmentQueries;
        return this;
    }
}
