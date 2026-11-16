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
package io.trino.plugin.iceberg.gpu;

import static java.util.Objects.requireNonNull;

/**
 * A predicate this connector enforces: {@code column operator constant}, over a null-free numeric
 * or date column, operators as in {@link io.trino.plugin.iceberg.gpu.engine.DeviceTable}.
 */
public record GpuPredicate(String column, int operator, double constant)
{
    public GpuPredicate
    {
        requireNonNull(column, "column is null");
    }
}
