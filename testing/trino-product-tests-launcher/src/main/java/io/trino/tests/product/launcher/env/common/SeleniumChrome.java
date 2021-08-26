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
package io.trino.tests.product.launcher.env.common;

import com.google.inject.Inject;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.testcontainers.PortBinder;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public class SeleniumChrome
        implements EnvironmentExtender
{
    private static final DockerImageName CHROME_IMAGE = DockerImageName.parse("selenium/standalone-chrome-debug").withTag("3.141.59-20210804");
    private static final int SELENIUM_PORT = 7777;
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(15);

    private final PortBinder portBinder;

    @Inject
    public SeleniumChrome(PortBinder portBinder)
    {
        this.portBinder = requireNonNull(portBinder, "portBinder is null");
    }

    @Override
    public void extendEnvironment(Environment.Builder builder)
    {
        DockerContainer seleniumChrome = new DockerContainer(CHROME_IMAGE.asCanonicalNameString(), "selenium-chrome");
        seleniumChrome.addEnv("SE_OPTS", "-port " + SELENIUM_PORT);
        seleniumChrome.addEnv("TZ", "Etc/UTC");
        seleniumChrome.addEnv("no_proxy", "localhost");
        seleniumChrome.setCommand("/opt/bin/entry_point.sh");

        seleniumChrome.waitingForAll(
                new LogMessageWaitStrategy()
                        .withRegEx(".*(RemoteWebDriver instances should connect to|Selenium Server is up and running).*\n")
                        .withStartupTimeout(STARTUP_TIMEOUT),
                new HostPortWaitStrategy()
                        .withStartupTimeout(STARTUP_TIMEOUT));

        // Taken from BrowserWebDriverContainer were it has been said that:
        // "Some unreliability of the selenium browser containers has been observed, so allow multiple attempts to start."
        seleniumChrome.setStartupAttempts(3);

        portBinder.exposePort(seleniumChrome, SELENIUM_PORT);

        builder.addContainer(seleniumChrome);
    }
}
