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
package io.prestosql.tests.utils;

import com.google.common.util.concurrent.Uninterruptibles;
import io.airlift.log.Logger;

import java.time.Duration;

public class TestUninterruptably
{
    private static final Logger log = Logger.get(TestUninterruptably.class);

    private TestUninterruptably() {}

    // A method used only during IDE test debugging to keep the docker containers alive.
    // Stopping at a breakpoint alone is not sufficient to keep containers alive indefinitely.
    public static void testUninterruptibly(Runnable runnable)
    {
        try {
            runnable.run();
        }
        catch (Throwable e) {
            log.error(e, "Saw exception %s", e);
            Uninterruptibles.sleepUninterruptibly(Duration.ofDays(1));
        }
        Uninterruptibles.sleepUninterruptibly(Duration.ofDays(1));
    }
}
