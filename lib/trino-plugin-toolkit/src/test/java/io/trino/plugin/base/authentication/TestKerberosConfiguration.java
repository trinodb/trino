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
package io.trino.plugin.base.authentication;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.plugin.base.authentication.KerberosConfiguration.Builder.getServerPrincipal;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKerberosConfiguration
{
    private static final String HOST_NAME = "host_name";

    @Test(dataProvider = "kerberosPrincipalPattern")
    public void testHostnameSubstitution(String actual, String expected)
    {
        assertThat(getServerPrincipal(actual, HOST_NAME)).isEqualTo(expected);
    }

    @DataProvider(name = "kerberosPrincipalPattern")
    public Object[][] kerberosPrincipalPattern()
    {
        return new Object[][] {
                {"server/_HOST@REALM.COM", "server/host_name@REALM.COM"},
                {"server/_HOST", "server/host_name"},
                {"server/trino-worker@REALM.COM", "server/trino-worker@REALM.COM"},
                {"server/trino-worker", "server/trino-worker"},
                {"SERVER_HOST/_HOST@REALM.COM", "SERVER_HOST/host_name@REALM.COM"},
                {"SERVER_HOST/_HOST", "SERVER_HOST/host_name"},
        };
    }
}
