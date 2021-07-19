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
package io.trino.testing;

import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

record FlakyTestRetryState(
        int retry,
        int maxRetries,
        String issue,
        Pattern retryAllowingPattern,
        boolean testSucceeded,
        boolean testFailed)
{
    FlakyTestRetryState(int maxRetries, String issue, Pattern retryAllowingPattern)
    {
        this(0, maxRetries, issue, retryAllowingPattern, false, false);
    }

    FlakyTestRetryState
    {
        checkArgument(retry >= 0, "retry is less than 0");
        checkArgument(maxRetries >= 0, "maxRetries is less than 0");
        requireNonNull(issue, "issue is null");
        requireNonNull(retryAllowingPattern, "retryAllowingPattern is null");
    }

    public boolean isTestFailed()
    {
        return testFailed;
    }

    public boolean shouldRetryTest()
    {
        if (testSucceeded || testFailed) {
            return false;
        }

        return retry <= maxRetries;
    }

    public FlakyTestRetryState testSuccess()
    {
        return new FlakyTestRetryState(retry + 1, maxRetries, issue, retryAllowingPattern, true, testFailed);
    }

    public FlakyTestRetryState testFailure(Throwable thrownByTest)
    {
        return new FlakyTestRetryState(
                retry + 1,
                maxRetries,
                issue,
                retryAllowingPattern,
                testSucceeded,
                !retryAllowingPattern.matcher(getStackTraceAsString(thrownByTest)).find());
    }
}
