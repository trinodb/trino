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
package io.trino.spi.connector;

import static java.util.Objects.requireNonNull;

public class LimitApplicationResult<T>
{
    private final T handle;
    private final boolean limitGuaranteed;
    private final boolean precalculateStatistics;

    /**
     * @param precalculateStatistics Indicates whether engine should consider calculating statistics based on the plan before pushdown,
     * as the connector may be unable to provide good table statistics for {@code handle}.
     */
    public LimitApplicationResult(T handle, boolean limitGuaranteed, boolean precalculateStatistics)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.limitGuaranteed = limitGuaranteed;
        this.precalculateStatistics = precalculateStatistics;
    }

    public T getHandle()
    {
        return handle;
    }

    public boolean isLimitGuaranteed()
    {
        return limitGuaranteed;
    }

    public boolean isPrecalculateStatistics()
    {
        return precalculateStatistics;
    }
}
