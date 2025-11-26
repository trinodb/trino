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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import reactor.core.publisher.Mono;

import java.util.concurrent.Semaphore;

import static com.google.common.base.Verify.verify;

public final class ConcurrencyLimitHttpPipelinePolicy
        implements HttpPipelinePolicy
{
    private final Semaphore semaphore;

    public ConcurrencyLimitHttpPipelinePolicy(int maxHttpRequests)
    {
        verify(maxHttpRequests > 0, "maxHttpRequests must be greater than 0");
        this.semaphore = new Semaphore(maxHttpRequests);
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next)
    {
        try {
            semaphore.acquire();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Mono.error(e);
        }
        return next.process().doFinally(_ -> semaphore.release());
    }
}
