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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.collect.ImmutableList;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.StageId;
import io.trino.execution.scheduler.faulttolerant.EventDrivenFaultTolerantQueryScheduler.StageExecution;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public class CompositeOutputDataSizeEstimator
        implements OutputDataSizeEstimator
{
    public static class Factory
            implements OutputDataSizeEstimatorFactory
    {
        private final List<OutputDataSizeEstimatorFactory> delegateFactories;

        @Inject
        public Factory(@ForCompositeOutputDataSizeEstimator List<OutputDataSizeEstimatorFactory> delegateFactories)
        {
            checkArgument(!delegateFactories.isEmpty(), "Got empty list of delegates");
            this.delegateFactories = ImmutableList.copyOf(delegateFactories);
        }

        @Override
        public OutputDataSizeEstimator create(Session session)
        {
            List<OutputDataSizeEstimator> estimators = delegateFactories.stream().map(factory -> factory.create(session))
                    .collect(toImmutableList());
            return new CompositeOutputDataSizeEstimator(estimators);
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForCompositeOutputDataSizeEstimator {}

    private final List<OutputDataSizeEstimator> estimators;

    private CompositeOutputDataSizeEstimator(List<OutputDataSizeEstimator> estimators)
    {
        this.estimators = ImmutableList.copyOf(estimators);
    }

    @Override
    public Optional<OutputDataSizeEstimateResult> getEstimatedOutputDataSize(
            StageExecution stageExecution,
            Function<StageId, StageExecution> stageExecutionLookup,
            boolean parentEager)
    {
        for (OutputDataSizeEstimator estimator : estimators) {
            Optional<OutputDataSizeEstimateResult> result = estimator.getEstimatedOutputDataSize(stageExecution, stageExecutionLookup, parentEager);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }
}
