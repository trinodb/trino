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
package io.trino.operator.join.unspilled;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.join.JoinBridge;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.OuterPositionIterator;
import io.trino.spi.type.Type;

import java.util.List;

public interface LookupSourceFactory
        extends JoinBridge
{
    List<Type> getTypes();

    List<Type> getOutputTypes();

    ListenableFuture<LookupSourceProvider> createLookupSourceProvider();

    int partitions();

    /**
     * Can be called only after {@link #createLookupSourceProvider()} is done and all users of {@link LookupSource}-s finished.
     */
    @Override
    OuterPositionIterator getOuterPositionIterator();

    @Override
    void destroy();

    default ListenableFuture<Void> isDestroyed()
    {
        throw new UnsupportedOperationException();
    }
}
