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
package io.trino.plugin.varada.metrics;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.annotations.VisibleForTesting;

import java.nio.LongBuffer;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public abstract class VaradaNativeStatsBase
        extends VaradaStatsBase
{
    protected final boolean needBuffer;
    protected LongBuffer buffer;

    protected VaradaNativeStatsBase(boolean needBuffer, String jmxKey)
    {
        super(jmxKey, VaradaStatType.Worker);
        this.needBuffer = needBuffer;
    }

    public boolean needBuffer()
    {
        return needBuffer;
    }

    public abstract int getNumberOfMetrics();

    public abstract void mergeWithBuffer(VaradaStatsBase varadaStatsBase);

    @VisibleForTesting
    public LongBuffer getBuffer()
    {
        return buffer;
    }

    public void setBuffer(LongBuffer buffer)
    {
        this.buffer = buffer;
    }
}
