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

import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;

public interface OperatorFactory
{
    Operator createOperator(DriverContext driverContext);

    /**
     * Declare that createOperator will not be called any more and release
     * any resources associated with this factory.
     * <p>
     * This method will be called only once.
     * Implementation doesn't need to worry about duplicate invocations.
     */
    void noMoreOperators();

    OperatorFactory duplicate();

    /**
     * Returns a future indicating that any dependencies operators have on other pipelines has been satisfied and that leaf splits
     * should be allowed to start for this operator. This is used to prevent join probe splits from starting before the build side
     * of a join is ready when the two are in the same stage (i.e.: broadcast join on top of a table scan).
     */
    default ListenableFuture<Void> pipelineDependenciesSatisfied()
    {
        return immediateVoidFuture();
    }
}
