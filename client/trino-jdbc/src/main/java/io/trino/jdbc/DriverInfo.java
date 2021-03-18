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
package io.trino.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.Resources.getResource;
import static java.lang.Integer.parseInt;

final class DriverInfo
{
    static final String DRIVER_NAME;
    static final String DRIVER_VERSION;
    static final int DRIVER_VERSION_MAJOR;
    static final int DRIVER_VERSION_MINOR;

    static {
        try {
            Properties properties = new Properties();
            URL url = getResource(DriverInfo.class, "driver.properties");
            try (InputStream in = url.openStream()) {
                properties.load(in);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            DRIVER_NAME = properties.getProperty("driverName");
            DRIVER_VERSION = properties.getProperty("driverVersion");

            verify(!isNullOrEmpty(DRIVER_NAME), "driverName is null or empty");
            verify(!isNullOrEmpty(DRIVER_VERSION), "driverVersion is null or empty");

            Matcher matcher = Pattern.compile("^(\\d+)(\\.(\\d+))?($|[.-])").matcher(DRIVER_VERSION);
            verify(matcher.find(), "driverVersion is invalid: %s", DRIVER_VERSION);

            DRIVER_VERSION_MAJOR = parseInt(matcher.group(1));
            DRIVER_VERSION_MINOR = parseInt(firstNonNull(matcher.group(3), "0"));
        }
        catch (RuntimeException e) {
            // log message since DriverManager hides initialization exceptions
            Logger.getLogger(TrinoDriver.class.getPackage().getName())
                    .log(Level.SEVERE, "Failed to load driver info", e);
            throw e;
        }
    }

    private DriverInfo() {}
}
