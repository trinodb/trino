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
package io.trino.spi.exchange;

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ExchangeSourceSplitter
        extends Closeable
{
    /**
     * Returns a future that will be completed when the splitter becomes unblocked.
     */
    CompletableFuture<?> isBlocked();

    /**
     * Returns next sub partition, or {@link Optional#empty()} if the splitting process is finished.
     */
    Optional<ExchangeSourceHandle> getNext();

    @Override
    void close();
}
