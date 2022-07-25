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
package io.trino.operator;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.spi.metrics.Metrics;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

// TODO: support spill
public interface WorkProcessorSourceOperator
        extends WorkProcessorOperator
{
    Supplier<Optional<UpdatablePageSource>> getUpdatablePageSourceSupplier();

    default DataSize getPhysicalInputDataSize()
    {
        return DataSize.ofBytes(0);
    }

    default long getPhysicalInputPositions()
    {
        return 0;
    }

    default DataSize getInternalNetworkInputDataSize()
    {
        return DataSize.ofBytes(0);
    }

    default long getInternalNetworkPositions()
    {
        return 0;
    }

    default DataSize getInputDataSize()
    {
        return DataSize.ofBytes(0);
    }

    default long getInputPositions()
    {
        return 0;
    }

    default Duration getReadTime()
    {
        return new Duration(0, NANOSECONDS);
    }

    default long getDynamicFilterSplitsProcessed()
    {
        return 0;
    }

    Metrics getConnectorMetrics();
}
