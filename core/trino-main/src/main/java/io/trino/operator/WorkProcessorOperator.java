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

import io.trino.spi.Page;
import io.trino.spi.metrics.Metrics;

import java.util.Optional;

// TODO: support spill
public interface WorkProcessorOperator
        extends AutoCloseable
{
    WorkProcessor<Page> getOutputPages();

    /**
     * Returns {@link OperatorInfo}. This method must be thread safe.
     * This method might be called after operator is closed to obtain
     * final {@link OperatorInfo}.
     */
    default Optional<OperatorInfo> getOperatorInfo()
    {
        return Optional.empty();
    }

    default Metrics getMetrics()
    {
        return Metrics.EMPTY;
    }

    @Override
    default void close()
            throws Exception
    {
        // do nothing
    }
}
