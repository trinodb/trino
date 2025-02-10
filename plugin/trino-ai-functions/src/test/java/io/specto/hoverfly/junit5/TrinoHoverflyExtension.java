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
package io.specto.hoverfly.junit5;

import io.specto.hoverfly.junit.core.Hoverfly;
import io.specto.hoverfly.junit.core.HoverflyMode;
import io.specto.hoverfly.junit.core.SimulationSource;
import io.specto.hoverfly.junit5.api.HoverflyCapture;
import io.specto.hoverfly.junit5.api.HoverflyConfig;
import io.specto.hoverfly.junit5.api.HoverflySimulate;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.lang.reflect.AnnotatedElement;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.specto.hoverfly.junit.core.HoverflyConstants.DEFAULT_HOVERFLY_EXPORT_PATH;
import static io.specto.hoverfly.junit.core.HoverflyConstants.DEFAULT_HOVERFLY_RESOURCE_DIR;
import static io.specto.hoverfly.junit5.HoverflyExtensionUtils.getHoverflyConfigs;

/**
 * Simple version of {@link HoverflyExtension} that supports the
 * {@link HoverflySimulate} or {@link HoverflyCapture} annotations using
 * separate simulation files for each test method.
 */
public class TrinoHoverflyExtension
        implements AfterEachCallback, BeforeEachCallback, AfterAllCallback, BeforeAllCallback, ParameterResolver
{
    private Hoverfly hoverfly;
    private HoverflyMode mode;
    private Path capturePath;

    @Override
    public void beforeAll(ExtensionContext context)
    {
        if (context.getExecutionMode() == ExecutionMode.CONCURRENT) {
            throw new RuntimeException("Hoverfly extension does not support concurrent test execution.");
        }

        AnnotatedElement element = context.getElement()
                .orElseThrow(() -> new RuntimeException("No test class found."));

        HoverflyConfig config;
        if (element.getAnnotation(HoverflySimulate.class) != null) {
            config = element.getAnnotation(HoverflySimulate.class).config();
            mode = HoverflyMode.SIMULATE;
        }
        else if (element.getAnnotation(HoverflyCapture.class) != null) {
            config = element.getAnnotation(HoverflyCapture.class).config();
            mode = HoverflyMode.CAPTURE;
        }
        else {
            throw new RuntimeException("No Hoverfly annotation found on " + element);
        }

        if (hoverfly == null) {
            hoverfly = new Hoverfly(getHoverflyConfigs(config), mode);
            hoverfly.start();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context)
    {
        String testName = "%s.%s.json".formatted(
                context.getRequiredTestClass().getSimpleName(),
                context.getRequiredTestMethod().getName());

        SimulationSource source = null;
        switch (mode) {
            case SIMULATE -> {
                String name = DEFAULT_HOVERFLY_RESOURCE_DIR + "/" + testName;
                URL url = context.getRequiredTestClass().getClassLoader().getResource(name);
                source = (url != null) ? SimulationSource.url(url) : SimulationSource.empty();
            }
            case CAPTURE -> capturePath = Paths.get(DEFAULT_HOVERFLY_EXPORT_PATH).resolve(testName);
            default -> throw new AssertionError("Unexpected value: " + mode);
        }

        if (hoverfly != null) {
            hoverfly.reset();
            if (mode.allowSimulationImport()) {
                hoverfly.simulate(source);
            }
        }
    }

    @Override
    public void afterEach(ExtensionContext context)
    {
        if (hoverfly != null && capturePath != null) {
            if (!hoverfly.getSimulation().getHoverflyData().getPairs().isEmpty()) {
                hoverfly.exportSimulation(capturePath);
            }
            capturePath = null;
        }
    }

    @Override
    public void afterAll(ExtensionContext context)
    {
        if (hoverfly != null) {
            hoverfly.close();
            hoverfly = null;
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    {
        return Hoverfly.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
    {
        return hoverfly;
    }
}
