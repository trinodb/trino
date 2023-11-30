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
package io.trino.plugin.kudu;

import io.airlift.log.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.util.Optional;

public class ProgressLoggingWatcher
        implements TestWatcher
{
    private final Logger logger = Logger.get(ProgressLoggingWatcher.class);

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason)
    {
        logger.info("Test Disabled: " + getDisplayName(context));
    }

    @Override
    public void testSuccessful(ExtensionContext context)
    {
        logger.info("Test Successful: " + getDisplayName(context));
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause)
    {
        logger.info("Test Aborted: " + getDisplayName(context));
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause)
    {
        logger.info("Test Failed: " + getDisplayName(context));
    }

    private String getDisplayName(ExtensionContext context)
    {
        return context.getTestClass().orElseThrow().getName() + "." + context.getTestMethod().orElseThrow().getName();
    }
}
