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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableMap;
import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.security.SystemAccessControl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpaAccessControlFactory
{
    @Test
    public void testCreatesSimpleAuthorizerIfNoBatchUriProvided()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();
        SystemAccessControl opaAuthorizer = factory.create(ImmutableMap.of("opa.policy.uri", "foo"));

        assertThat(opaAuthorizer).isInstanceOf(OpaAccessControl.class);
        assertThat(opaAuthorizer).isNotInstanceOf(OpaBatchAccessControl.class);
    }

    @Test
    public void testCreatesBatchAuthorizerIfBatchUriProvided()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();
        SystemAccessControl opaAuthorizer = factory.create(
                ImmutableMap.<String, String>builder()
                        .put("opa.policy.uri", "foo")
                        .put("opa.policy.batched-uri", "bar")
                        .buildOrThrow());

        assertThat(opaAuthorizer).isInstanceOf(OpaBatchAccessControl.class);
        assertThat(opaAuthorizer).isInstanceOf(OpaAccessControl.class);
    }

    @Test
    public void testBasePolicyUriCannotBeUnset()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();

        assertThatThrownBy(() -> factory.create(ImmutableMap.of())).isInstanceOf(ApplicationConfigurationException.class);
    }

    @Test
    public void testConfigMayNotBeNull()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();

        assertThatThrownBy(() -> factory.create(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    public void testSupportsAirliftHttpConfigs()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();
        SystemAccessControl opaAuthorizer = factory.create(
                ImmutableMap.<String, String>builder()
                        .put("opa.policy.uri", "foo")
                        .put("opa.http-client.log.enabled", "true")
                        .buildOrThrow());

        assertThat(opaAuthorizer).isInstanceOf(OpaAccessControl.class);
        assertThat(opaAuthorizer).isNotInstanceOf(OpaBatchAccessControl.class);
    }
}
