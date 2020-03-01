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

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.output.OutputFrame.OutputType.END;

public final class PrintingLogConsumer
        extends BaseConsumer<PrintingLogConsumer>
{
    private static final Logger log = Logger.get(PrintingLogConsumer.class);

    private final String prefix;

    private final PrintStream out;

    public PrintingLogConsumer(String prefix)
    {
        this.prefix = requireNonNull(prefix, "prefix is null");

        try {
            // write directly to System.out, bypassing logging & io.airlift.log.Logging#rewireStdStreams
            this.out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
        }
        catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
    }
}
