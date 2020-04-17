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
package io.prestosql.tests.product.launcher.testcontainers;

import io.airlift.log.Logger;
import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

import java.io.PrintStream;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.output.OutputFrame.OutputType.END;

public final class PrintingLogConsumer
        extends BaseConsumer<PrintingLogConsumer>
{
    private static final Logger log = Logger.get(PrintingLogConsumer.class);

    private final PrintStream out;
    private final String prefix;

    public PrintingLogConsumer(PrintStream out, String prefix)
    {
        this.out = requireNonNull(out, "out is null");
        this.prefix = requireNonNull(prefix, "prefix is null");
    }

    @Override
    public void accept(OutputFrame outputFrame)
    {
        // Sanitize message. This mimics code in org.testcontainers.containers.output.Slf4jLogConsumer#accept
        String message = outputFrame.getUtf8String().replaceAll("\\r?\\n?$", "");
        if (message.contains("\n")) {
            log.warn("Message contains newline character: [%s]", message);
        }

        if (!message.isEmpty() || outputFrame.getType() != END) {
            out.println(prefix + message);
        }
        if (outputFrame.getType() == END) {
            out.println(prefix + "(exited)");
        }
        out.flush();
    }
}
