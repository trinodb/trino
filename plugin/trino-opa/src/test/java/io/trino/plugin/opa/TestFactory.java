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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestFactory
{
    @Test
    public void testCreatesSimpleAuthorizerIfNoBatchUriProvided()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();
        SystemAccessControl opaAuthorizer = factory.create(ImmutableMap.of("opa.policy.uri", "foo"));

        assertInstanceOf(OpaAccessControl.class, opaAuthorizer);
        assertFalse(opaAuthorizer instanceof OpaBatchAccessControl);
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

        assertInstanceOf(OpaBatchAccessControl.class, opaAuthorizer);
        assertInstanceOf(OpaAccessControl.class, opaAuthorizer);
    }

    @Test
    public void testBasePolicyUriCannotBeUnset()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();

        assertThrows(
                ApplicationConfigurationException.class,
                () -> factory.create(ImmutableMap.of()),
                "may not be null");
    }

    @Test
    public void testConfigMayNotBeNull()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();

        assertThrows(
                NullPointerException.class,
                () -> factory.create(null));
    }
}
