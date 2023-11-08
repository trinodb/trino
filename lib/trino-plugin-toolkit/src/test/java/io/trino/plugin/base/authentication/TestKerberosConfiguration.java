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

import org.testng.annotations.Test;

import static io.trino.plugin.base.authentication.KerberosConfiguration.Builder.getServerPrincipal;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKerberosConfiguration
{
    private static final String HOST_NAME = "host_name";

    @Test
    public void testHostnameSubstitution()
    {
        assertThat(getServerPrincipal("server/_HOST@REALM.COM", HOST_NAME)).isEqualTo("server/host_name@REALM.COM");
        assertThat(getServerPrincipal("server/_HOST", HOST_NAME)).isEqualTo("server/host_name");
        assertThat(getServerPrincipal("server/trino-worker@REALM.COM", HOST_NAME)).isEqualTo("server/trino-worker@REALM.COM");
        assertThat(getServerPrincipal("server/trino-worker", HOST_NAME)).isEqualTo("server/trino-worker");
        assertThat(getServerPrincipal("SERVER_HOST/_HOST@REALM.COM", HOST_NAME)).isEqualTo("SERVER_HOST/host_name@REALM.COM");
        assertThat(getServerPrincipal("SERVER_HOST/_HOST", HOST_NAME)).isEqualTo("SERVER_HOST/host_name");
    }
}
