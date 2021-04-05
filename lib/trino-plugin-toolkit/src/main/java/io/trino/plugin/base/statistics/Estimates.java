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
package io.trino.plugin.base.statistics;

import io.trino.spi.statistics.Estimate;

import java.util.OptionalLong;

public final class Estimates
{
    private Estimates() {}

    public static Estimate toEstimate(OptionalLong value)
    {
        if (value.isPresent()) {
            return Estimate.of(value.getAsLong());
        }
        return Estimate.unknown();
    }
}
