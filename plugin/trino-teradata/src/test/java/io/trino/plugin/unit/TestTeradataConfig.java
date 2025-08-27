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

package io.trino.plugin.unit;

import io.trino.plugin.teradata.TeradataConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTeradataConfig
{
    @Test
    public void testDefaults()
    {
        TeradataConfig config = new TeradataConfig();
        assertThat(config.getOidcClientId()).isEmpty();
        assertThat(config.getOidcClientSecret()).isEmpty();
        assertThat(config.getOidcJWSCertificate()).isEmpty();
        assertThat(config.getOidcJWSPrivateKey()).isEmpty();
        assertThat(config.getOidcJwtToken()).isEmpty();
        assertThat(config.getLogMech()).isEqualTo("TD2");
        assertThat(config.getTeradataCaseSensitivity()).isEqualTo(TeradataConfig.TeradataCaseSensitivity.CASE_SENSITIVE);
    }

    @Test
    public void testSetters()
    {
        TeradataConfig config = new TeradataConfig()
                .setOidcClientId("clientId")
                .setOidcClientSecret("clientSecret")
                .setOidcJWSCertificate("certificate")
                .setOidcJWSPrivateKey("privateKey")
                .setOidcJwtToken("jwtToken")
                .setLogMech("LDAP")
                .setTeradataCaseSensitivity(TeradataConfig.TeradataCaseSensitivity.CASE_INSENSITIVE);

        assertThat(config.getOidcClientId()).contains("clientId");
        assertThat(config.getOidcClientSecret()).contains("clientSecret");
        assertThat(config.getOidcJWSCertificate()).contains("certificate");
        assertThat(config.getOidcJWSPrivateKey()).contains("privateKey");
        assertThat(config.getOidcJwtToken()).contains("jwtToken");
        assertThat(config.getLogMech()).isEqualTo("LDAP");
        assertThat(config.getTeradataCaseSensitivity()).isEqualTo(TeradataConfig.TeradataCaseSensitivity.CASE_INSENSITIVE);
    }

    @Test
    public void testTeradataCaseSensitivityEnum()
    {
        assertThat(TeradataConfig.TeradataCaseSensitivity.valueOf("CASE_INSENSITIVE"))
                .isEqualTo(TeradataConfig.TeradataCaseSensitivity.CASE_INSENSITIVE);
        assertThat(TeradataConfig.TeradataCaseSensitivity.valueOf("CASE_SENSITIVE"))
                .isEqualTo(TeradataConfig.TeradataCaseSensitivity.CASE_SENSITIVE);
        assertThat(TeradataConfig.TeradataCaseSensitivity.valueOf("AS_DEFINED"))
                .isEqualTo(TeradataConfig.TeradataCaseSensitivity.AS_DEFINED);
    }
}
