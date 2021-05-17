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
package io.trino.tests.product.utils;

import org.zeroturnaround.exec.ProcessExecutor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeoutException;

public final class Cli
{
    private Cli() {}

    public static String cli(String... command)
    {
        try {
            return new ProcessExecutor()
                    .command(command)
                    .exitValueNormal()
                    .readOutput(true)
                    .redirectErrorStream(false)
                    .execute()
                    .outputUTF8();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
