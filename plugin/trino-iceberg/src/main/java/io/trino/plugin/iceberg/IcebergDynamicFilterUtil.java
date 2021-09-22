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
package io.trino.plugin.iceberg;

import com.google.common.base.Stopwatch;
import io.airlift.log.Logger;
import io.trino.spi.connector.DynamicFilter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class IcebergDynamicFilterUtil
{
    private static final Logger log = Logger.get(IcebergDynamicFilterUtil.class);

    private IcebergDynamicFilterUtil() {}

    public static void waitDynamicFilterUntilTimeout(long timeout, DynamicFilter dynamicFilter)
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeLeft = timeout - stopwatch.elapsed(MILLISECONDS);
        try {
            while (timeLeft > 0 && dynamicFilter.isAwaitable()) {
                dynamicFilter.isBlocked().get(timeLeft, MILLISECONDS);
                // since DF may be narrowed down multiple times during query runtime,
                // we should loop until time out here
                timeLeft = timeout - stopwatch.elapsed(MILLISECONDS);
            }
        }
        catch (Exception e) {
            log.warn("Wait for dynamic filter exited with failure!", e);
        }

        log.debug("Dynamic Filter current predicate %s"
                + dynamicFilter.getCurrentPredicate().toString());
    }
}
