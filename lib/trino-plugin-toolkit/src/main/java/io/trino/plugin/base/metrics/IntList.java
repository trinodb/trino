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
package io.trino.plugin.base.metrics;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import io.trino.spi.metrics.Metric;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NONE) // Do not add @class property
@JsonSerialize
public record IntList(List<Integer> integers)
        implements Metric<IntList>
{
    public IntList
    {
        integers = ImmutableList.copyOf(integers);
    }

    @Override
    public IntList mergeWith(IntList other)
    {
        return new IntList(ImmutableList.<Integer>builder()
                .addAll(integers)
                .addAll(other.integers)
                .build());
    }
}
