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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Isolated;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Isolated // temporarily modifies JVM-wide system properties
public class TestKerberosNativeGssConnectionProperty
{
    private static final String NATIVE_GSS = "sun.security.jgss.native";
    private static final String USE_SUBJECT_CREDS_ONLY = "javax.security.auth.useSubjectCredsOnly";

    private static final String KERBEROS_URL = "jdbc:trino://localhost:8080?KerberosRemoteServiceName=HTTP";
    private static final String NATIVE_GSS_URL = KERBEROS_URL + "&KerberosUseNativeGSS=true";

    @Test
    public void testDefaultsToFalse()
            throws Throwable
    {
        withSystemProperties(null, null, () ->
                assertThat(createDriverUri(KERBEROS_URL).getKerberosUseNativeGss()).isFalse());
    }

    @Test
    public void testRequiresNativeGssSystemProperty()
            throws Throwable
    {
        String message = "Connection property KerberosUseNativeGSS requires system property sun.security.jgss.native to be set to true";
        withSystemProperties(null, "false", () -> assertInvalid(NATIVE_GSS_URL, message));
        withSystemProperties("false", "false", () -> assertInvalid(NATIVE_GSS_URL, message));
    }

    @Test
    public void testRequiresUseSubjectCredsOnlySystemProperty()
            throws Throwable
    {
        String message = "Connection property KerberosUseNativeGSS requires system property javax.security.auth.useSubjectCredsOnly to be set to false";
        withSystemProperties("true", null, () -> assertInvalid(NATIVE_GSS_URL, message));
        withSystemProperties("true", "true", () -> assertInvalid(NATIVE_GSS_URL, message));
    }

    @Test
    public void testEnabledWhenPrerequisitesArePresent()
            throws Throwable
    {
        // JGSS treats the value case-insensitively, so FALSE must be accepted
        withSystemProperties("true", "FALSE", () ->
                assertThat(createDriverUri(NATIVE_GSS_URL).getKerberosUseNativeGss()).isTrue());
    }

    @Test
    public void testRejectsExplicitCredentialProperties()
            throws Throwable
    {
        Map<String, String> incompatibleProperties = ImmutableMap.of(
                "KerberosPrincipal", "test",
                "KerberosConfigPath", "/etc/krb5.conf",
                "KerberosKeytabPath", "/etc/krb5.keytab",
                "KerberosCredentialCachePath", "/tmp/krb5cc");
        withSystemProperties("true", "false", () -> {
            for (Map.Entry<String, String> entry : incompatibleProperties.entrySet()) {
                assertInvalid(
                        format("%s&%s=%s", NATIVE_GSS_URL, entry.getKey(), entry.getValue()),
                        format("Connection property %s cannot be set if KerberosUseNativeGSS is enabled", entry.getKey()));
            }
        });
    }

    private static void withSystemProperties(String nativeGss, String useSubjectCredsOnly, Executable action)
            throws Throwable
    {
        String originalNativeGss = System.getProperty(NATIVE_GSS);
        String originalUseSubjectCredsOnly = System.getProperty(USE_SUBJECT_CREDS_ONLY);
        try {
            setOrClearProperty(NATIVE_GSS, nativeGss);
            setOrClearProperty(USE_SUBJECT_CREDS_ONLY, useSubjectCredsOnly);
            action.execute();
        }
        finally {
            setOrClearProperty(NATIVE_GSS, originalNativeGss);
            setOrClearProperty(USE_SUBJECT_CREDS_ONLY, originalUseSubjectCredsOnly);
        }
    }

    private static void setOrClearProperty(String key, String value)
    {
        if (value == null) {
            System.clearProperty(key);
        }
        else {
            System.setProperty(key, value);
        }
    }

    private static TrinoDriverUri createDriverUri(String url)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", "test");
        return TrinoDriverUri.createDriverUri(url, properties);
    }

    private static void assertInvalid(String url, String message)
    {
        assertThatThrownBy(() -> createDriverUri(url))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining(message);
    }
}
