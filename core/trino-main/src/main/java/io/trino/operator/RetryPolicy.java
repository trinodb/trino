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
package io.trino.operator;

import io.trino.spi.connector.RetryMode;

public enum RetryPolicy
{
    TASK(RetryMode.RETRIES_ENABLED),
    QUERY(RetryMode.RETRIES_ENABLED),
    NONE(RetryMode.NO_RETRIES),
    /**/;

    private final RetryMode retryMode;

    RetryPolicy(RetryMode retryMode)
    {
        this.retryMode = retryMode;
    }

    public RetryMode getRetryMode()
    {
        return this.retryMode;
    }
}
