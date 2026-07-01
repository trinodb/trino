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
package io.trino.operator.aggregation.state;

import io.trino.spi.function.AccumulatorState;

/// Accumulator for a long-form day-time interval: a microsecond count plus the picoseconds within the
/// last microsecond, kept normalized to `[0, 1_000_000)`. Used by the day-time interval `sum`, which
/// must carry sub-microsecond precision for an interval whose fractional-seconds precision exceeds 6.
public interface LongIntervalState
        extends AccumulatorState
{
    @InitialBooleanValue(true)
    boolean isNull();

    void setNull(boolean value);

    long getMicros();

    void setMicros(long value);

    long getPicosOfMicro();

    void setPicosOfMicro(long value);
}
