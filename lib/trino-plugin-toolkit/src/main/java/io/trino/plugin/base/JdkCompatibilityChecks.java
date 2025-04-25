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
package io.trino.plugin.base;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.inject.Binder;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdkCompatibilityChecks
{
    private final String inputArguments;

    private static final JdkCompatibilityChecks INSTANCE = new JdkCompatibilityChecks(
            ManagementFactory.getRuntimeMXBean().getInputArguments());

    @VisibleForTesting
    JdkCompatibilityChecks(List<String> inputArguments)
    {
        this.inputArguments = Joiner.on(" ")
                .skipNulls()
                .join(requireNonNull(inputArguments, "inputArguments is null"));
    }

    public static void verifyConnectorAccessOpened(Binder binder, String connectorName, Multimap<String, String> modules)
    {
        INSTANCE.verifyAccessOpened(wrap(binder), format("Connector '%s'", connectorName), modules);
    }

    public static void verifyConnectorUnsafeAllowed(Binder binder, String connectorName)
    {
        INSTANCE.verifyUnsafeAllowed(wrap(binder), format("Connector '%s'", connectorName));
    }

    @VisibleForTesting
    void verifyAccessOpened(ThrowableSettable throwableSettable, String description, Multimap<String, String> modules)
    {
        ImmutableList.Builder<String> missingJvmArguments = ImmutableList.builder();
        for (String fromModule : modules.keySet()) {
            for (String toModule : modules.get(fromModule)) {
                String requiredJvmArgument = format(".*?--add-opens[\\s=]%s/%s=ALL-UNNAMED.*?", Pattern.quote(fromModule), Pattern.quote(toModule));
                if (!inputArguments.matches(requiredJvmArgument)) {
                    missingJvmArguments.add(format("--add-opens=%s/%s=ALL-UNNAMED", fromModule, toModule));
                }
            }
        }

        List<String> requiredJvmArguments = missingJvmArguments.build();
        if (!requiredJvmArguments.isEmpty()) {
            throwableSettable.setThrowable(new IllegalStateException(format("%s requires additional JVM argument(s). Please add the following to the JVM configuration: '%s'", description, String.join(" ", requiredJvmArguments))));
        }
    }

    @VisibleForTesting
    void verifyUnsafeAllowed(ThrowableSettable throwableSettable, String description)
    {
        String requiredJvmArgument = "--sun-misc-unsafe-memory-access=allow";
        if (!inputArguments.matches(".*?%s.*?".formatted(Pattern.quote(requiredJvmArgument)))) {
            throwableSettable.setThrowable(new IllegalStateException(format("%s requires additional JVM argument(s). Please add the following to the JVM configuration: '%s'", description, requiredJvmArgument)));
        }
    }

    private static ThrowableSettable wrap(Binder binder)
    {
        return throwable -> binder.addError(throwable.getMessage());
    }

    @VisibleForTesting
    interface ThrowableSettable
    {
        void setThrowable(Throwable throwable);
    }
}
