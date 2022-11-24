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
package io.trino.plugin.base.type;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;

public record DecodedTimestamp(long epochSeconds, int nanosOfSecond)
{
    private static final long NANOS_PER_SECOND = SECONDS.toNanos(1);

    public DecodedTimestamp
    {
        checkArgument(nanosOfSecond >= 0 && nanosOfSecond < NANOS_PER_SECOND, "Invalid value for nanosOfSecond: %s", nanosOfSecond);
    }
}
