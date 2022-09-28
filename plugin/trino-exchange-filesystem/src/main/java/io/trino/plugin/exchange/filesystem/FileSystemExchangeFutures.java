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
package io.trino.plugin.exchange.filesystem;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;

public final class FileSystemExchangeFutures
{
    private FileSystemExchangeFutures() {}

    // Helper function that translates exception and transform future type to avoid abstraction leak
    public static ListenableFuture<Void> translateFailures(ListenableFuture<?> listenableFuture)
    {
        return asVoid(Futures.catchingAsync(listenableFuture, Throwable.class, throwable -> {
            if (throwable instanceof Error || throwable instanceof IOException) {
                return immediateFailedFuture(throwable);
            }
            return immediateFailedFuture(new IOException(throwable));
        }, directExecutor()));
    }
}
