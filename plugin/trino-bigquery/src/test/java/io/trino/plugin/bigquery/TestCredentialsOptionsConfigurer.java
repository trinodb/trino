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
package io.trino.plugin.bigquery;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Optional;

import static io.trino.plugin.bigquery.CredentialsOptionsConfigurer.resolveProjectId;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCredentialsOptionsConfigurer
{
    @Test
    public void testConfigurationOnly()
    {
        String projectId = resolveProjectId(Optional.of("pid"), Optional.empty());
        assertThat(projectId).isEqualTo("pid");
    }

    @Test
    public void testCredentialsOnly()
            throws Exception
    {
        String projectId = resolveProjectId(Optional.empty(), loadCredentials());
        assertThat(projectId).isEqualTo("presto-bq-credentials-test");
    }

    @Test
    public void testBothConfigurationAndCredentials()
            throws Exception
    {
        String projectId = resolveProjectId(Optional.of("pid"), loadCredentials());
        assertThat(projectId).isEqualTo("pid");
    }

    private static Optional<Credentials> loadCredentials()
            throws IOException
    {
        return Optional.of(GoogleCredentials.fromStream(Resources.getResource("test-account.json").openStream()));
    }
}
