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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.AnnotationParameterValueList;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import io.trino.plugin.base.config.ConfigPropertyMetadata;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPostgreSqlPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new PostgreSqlPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.of(
                        "connection-url", "jdbc:postgresql:test",
                        "bootstrap.quiet", "true"),
                new TestingPostgreSqlConnectorContext()).shutdown();
    }

    @Test
    void testUnknownPropertiesAreRedactable()
    {
        Plugin plugin = new PostgreSqlPlugin();
        ConnectorFactory connectorFactory = getOnlyElement(plugin.getConnectorFactories());
        Set<String> unknownProperties = ImmutableSet.of("unknown");

        Set<String> redactableProperties = connectorFactory.getRedactablePropertyNames(unknownProperties);

        assertThat(redactableProperties).isEqualTo(unknownProperties);
    }

    @Test
    void testSecuritySensitivePropertiesAreRedactable()
            throws Exception
    {
        // The purpose of this test is to help identify security-sensitive properties that
        // may be used by the connector. These properties are detected by scanning the
        // plugin's runtime classpath and collecting all property names annotated with
        // @ConfigSecuritySensitive. The scan includes all configuration classes, whether
        // they are always used, conditionally used, or never used. This approach has both
        // advantages and disadvantages.
        //
        // One advantage is that we don't need to rely on the plugin's configuration to
        // retrieve properties that are used conditionally. However, this method may also
        // capture properties that are not used at all but are pulled into the classpath
        // by dependencies. With that in mind, if this test fails, it likely indicates that
        // either a property needs to be added to the connector's security-sensitive
        // property names list, or it should be added to the excluded properties list below.
        Set<String> excludedClasses = ImmutableSet.of(
                "io.trino.plugin.base.ldap.LdapClientConfig",
                "io.airlift.http.client.HttpClientConfig",
                "io.airlift.node.NodeConfig",
                "io.airlift.log.LoggingConfiguration",
                "io.trino.plugin.base.security.FileBasedAccessControlConfig",
                "io.airlift.configuration.secrets.SecretsPluginConfig",
                "io.trino.plugin.base.jmx.ObjectNameGeneratorConfig");
        Plugin plugin = new PostgreSqlPlugin();
        ConnectorFactory connectorFactory = getOnlyElement(plugin.getConnectorFactories());

        Set<ConfigPropertyMetadata> propertiesFoundInClasspath = findPropertiesInRuntimeClasspath(excludedClasses);
        Set<String> allPropertyNames = propertiesFoundInClasspath.stream()
                .map(ConfigPropertyMetadata::name)
                .collect(toImmutableSet());
        Set<String> expectedProperties = propertiesFoundInClasspath.stream()
                .filter(ConfigPropertyMetadata::sensitive)
                .map(ConfigPropertyMetadata::name)
                .collect(toImmutableSet());

        Set<String> redactableProperties = connectorFactory.getRedactablePropertyNames(allPropertyNames);

        assertThat(redactableProperties).isEqualTo(expectedProperties);
    }

    private static Set<ConfigPropertyMetadata> findPropertiesInRuntimeClasspath(Set<String> excludedClassNames)
            throws URISyntaxException, IOException
    {
        try (ScanResult scanResult = new ClassGraph()
                .overrideClasspath(buildRuntimeClasspath())
                .enableAllInfo()
                .scan()) {
            return scanResult.getClassesWithMethodAnnotation(Config.class).stream()
                    .filter(classInfo -> !excludedClassNames.contains(classInfo.getName()))
                    .flatMap(classInfo -> classInfo.getMethodInfo().stream())
                    .filter(methodInfo -> methodInfo.hasAnnotation(Config.class))
                    .map(methodInfo -> {
                        boolean sensitive = methodInfo.hasAnnotation(ConfigSecuritySensitive.class);
                        AnnotationInfo annotationInfo = methodInfo.getAnnotationInfo(Config.class);
                        checkState(annotationInfo != null, "Missing @Config annotation for %s", methodInfo);
                        AnnotationParameterValueList parameterValues = annotationInfo.getParameterValues();
                        checkState(parameterValues.size() == 1, "Expected exactly one parameter for %s", annotationInfo);
                        String propertyName = (String) parameterValues.getFirst().getValue();
                        return new ConfigPropertyMetadata(propertyName, sensitive);
                    })
                    .collect(toImmutableSet());
        }
    }

    private static String buildRuntimeClasspath()
            throws URISyntaxException, IOException
    {
        // This file is generated by the maven-dependency-plugin, which is configured in the connector's pom.xml file.
        String runtimeDependenciesFile = "runtime-dependencies.txt";
        URL runtimeDependenciesUrl = TestPostgreSqlPlugin.class.getClassLoader().getResource(runtimeDependenciesFile);
        checkState(runtimeDependenciesUrl != null, "Missing %s file", runtimeDependenciesUrl);
        String runtimeDependenciesClasspath = Files.readString(Path.of(runtimeDependenciesUrl.toURI()));

        File classDirectory = new File(new File(runtimeDependenciesUrl.toURI()).getParentFile().getParentFile(), "classes/");
        checkState(classDirectory.exists(), "Missing %s directory", classDirectory.getAbsolutePath());

        return "%s:%s".formatted(runtimeDependenciesClasspath, classDirectory.getAbsolutePath());
    }
}
