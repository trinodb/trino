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
package io.trino.client.auth.external;

import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public final class MockTokenPoller
        implements TokenPoller
{
    private final Map<URI, Queue<TokenPollResult>> results = new HashMap<>();

    public MockTokenPoller withResult(URI tokenUri, TokenPollResult result)
    {
        results.compute(tokenUri, (uri, queue) -> {
            if (queue == null) {
                return new ArrayDeque<>(ImmutableList.of(result));
            }
            queue.add(result);
            return queue;
        });
        return this;
    }

    @Override
    public TokenPollResult pollForToken(URI tokenUri, Duration ignored)
    {
        Queue<TokenPollResult> queue = results.get(tokenUri);
        if (queue == null) {
            throw new IllegalArgumentException("Unknown token URI: " + tokenUri);
        }
        return queue.remove();
    }
}
