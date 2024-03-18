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
package io.trino.plugin.varada;

import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.trino.plugin.varada.VaradaSessionProperties.PREDICATE_SIMPLIFY_THRESHOLD;
import static io.trino.plugin.varada.VaradaSessionProperties.UNSUPPORTED_FUNCTIONS;
import static io.trino.plugin.varada.VaradaSessionProperties.UNSUPPORTED_NATIVE_FUNCTIONS;
import static io.trino.spi.expression.StandardFunctions.LIKE_FUNCTION_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VaradaSessionPropertiesTest
{
    @Test
    public void testNoNullDefaultValues()
    {
        List<String> allowedNullProps = List.of(
                VaradaSessionProperties.IMPORT_EXPORT_S3_PATH,
                VaradaSessionProperties.MATCH_COLLECT_CATALOG,
                VaradaSessionProperties.UNSUPPORTED_FUNCTIONS,
                VaradaSessionProperties.UNSUPPORTED_NATIVE_FUNCTIONS,
                VaradaSessionProperties.SPLIT_TO_WORKER,
                VaradaSessionProperties.ENABLE_DICTIONARY);
        VaradaSessionProperties varadaSessionProperties = new VaradaSessionProperties(new GlobalConfiguration());
        List<PropertyMetadata<?>> sessionProperties = varadaSessionProperties.getSessionProperties();
        sessionProperties.stream().filter(sessionProperty -> !allowedNullProps.contains(sessionProperty.getName())).forEach((property) -> assertThat(property.getDefaultValue()).isNotNull());
    }

    @Test
    public void testS3ImportExportPath()
    {
        String s3StorePath = VaradaSessionProperties.getS3ImportExportPath("s3://dev-andromeda-ws-backup-552728471257-uswt2/resiliency");
        assertThat(s3StorePath).isEqualTo("s3://dev-andromeda-ws-backup-552728471257-uswt2/resiliency/import_export");
    }

    @Test
    public void testUnsupportedFunctions()
    {
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        Set<String> unsupportedFunctions = VaradaSessionProperties.getUnsupportedFunctions(connectorSession, globalConfiguration);
        assertThat(unsupportedFunctions).isEqualTo(Set.of());

        globalConfiguration.setUnsupportedFunctions("  is_nan   , ceil  ");
        unsupportedFunctions = VaradaSessionProperties.getUnsupportedFunctions(connectorSession, globalConfiguration);
        assertThat(unsupportedFunctions).isEqualTo(Set.of("is_nan", "ceil"));

        when(connectorSession.getProperty(UNSUPPORTED_FUNCTIONS, String.class)).thenReturn("");
        unsupportedFunctions = VaradaSessionProperties.getUnsupportedFunctions(connectorSession, globalConfiguration);
        assertThat(unsupportedFunctions).isEqualTo(Set.of());

        when(connectorSession.getProperty(UNSUPPORTED_FUNCTIONS, String.class)).thenReturn("ceil, $like");
        unsupportedFunctions = VaradaSessionProperties.getUnsupportedFunctions(connectorSession, globalConfiguration);
        assertThat(unsupportedFunctions).isEqualTo(Set.of("ceil", LIKE_FUNCTION_NAME.getName()));
    }

    @Test
    public void testUnsupportedNativeFunctions()
    {
        NativeConfiguration nativeConfiguration = new NativeConfiguration();
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        Set<String> unsupportedNativeFunctions = VaradaSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfiguration);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of());

        nativeConfiguration.setUnsupportedNativeFunctions("  is_nan   ,  ceil   ");
        unsupportedNativeFunctions = VaradaSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfiguration);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of("is_nan", "ceil"));

        when(connectorSession.getProperty(UNSUPPORTED_NATIVE_FUNCTIONS, String.class)).thenReturn("");
        unsupportedNativeFunctions = VaradaSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfiguration);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of());

        when(connectorSession.getProperty(UNSUPPORTED_NATIVE_FUNCTIONS, String.class)).thenReturn("  ceil ,  $greater_than   ");
        unsupportedNativeFunctions = VaradaSessionProperties.getUnsupportedNativeFunctions(connectorSession, nativeConfiguration);
        assertThat(unsupportedNativeFunctions).isEqualTo(Set.of("ceil", "$greater_than"));
    }

    @Test
    public void testGetPredicateSimplifyThreshold()
    {
        ConnectorSession connectorSession = mock(ConnectorSession.class);
        GlobalConfiguration globalConfiguration = new GlobalConfiguration();

        assertThat(VaradaSessionProperties.getPredicateSimplifyThreshold(connectorSession, globalConfiguration))
                .isEqualTo(globalConfiguration.getPredicateSimplifyThreshold());

        when(connectorSession.getProperty(eq(PREDICATE_SIMPLIFY_THRESHOLD), eq(Integer.class))).thenReturn(1);

        assertThat(VaradaSessionProperties.getPredicateSimplifyThreshold(connectorSession, globalConfiguration))
                .isEqualTo(1);
    }
}
