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
package io.trino.testing.services.junit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FlakyRetryFixture
{
    static int attempts;
    static boolean interruptedAfterEach;

    @Test
    @Flaky(issue = "intentionally flaky for @Flaky test purposes", match = "MATCHING_FAILURE")
    void test()
    {
        attempts++;
        if (attempts == 1) {
            try {
                throw new InterruptedException("original interruption");
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("MATCHING_FAILURE", e);
            }
        }
    }

    @AfterEach
    void afterEach()
    {
        interruptedAfterEach = Thread.currentThread().isInterrupted();
    }
}
