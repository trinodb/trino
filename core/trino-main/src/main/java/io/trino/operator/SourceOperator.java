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

import io.trino.metadata.Split;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.function.Supplier;

public interface SourceOperator
        extends Operator
{
    PlanNodeId getSourceId();

    Supplier<Optional<UpdatablePageSource>> addSplit(Split split);

    void noMoreSplits();

    /**
     * This method will be called to unblock any blocking operation that operator is doing.
     * In order to unblock blocking operation this method is called in separate thread.
     * Once this method is called, it is known that soon {@link close()} method will be called,
     * so output is no longer needed.
     */
    default void cancel() {}
}
