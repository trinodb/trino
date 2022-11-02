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

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.nio.charset.StandardCharsets.UTF_8;

// Separate class to isolate static initialization
final class SpiVersionHolder
{
    private SpiVersionHolder() {}

    static final String SPI_COMPILE_TIME_VERSION;

    static {
        try {
            String spiVersion = Resources.toString(Resources.getResource(SpiVersionHolder.class, "trino-spi-compile-time-version.txt"), UTF_8);
            if (spiVersion.isBlank()) {
                throw new IllegalStateException("Malformed version resource");
            }
            SPI_COMPILE_TIME_VERSION = spiVersion.strip();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
