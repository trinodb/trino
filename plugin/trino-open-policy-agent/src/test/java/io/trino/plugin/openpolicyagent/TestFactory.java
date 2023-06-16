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
package io.trino.plugin.openpolicyagent;

import io.airlift.bootstrap.ApplicationConfigurationException;
import io.trino.spi.security.SystemAccessControl;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestFactory
{
    @Test
    public void testCreatesSimpleAuthorizerIfNoBatchUriProvided()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();
        SystemAccessControl opaAuthorizer = factory.create(Map.of("opa.policy.uri", "foo"));

        assertInstanceOf(OpaAccessControl.class, opaAuthorizer);
        assertFalse(opaAuthorizer instanceof OpaBatchAccessControl);
    }

    @Test
    public void testCreatesBatchAuthorizerIfBatchUriProvided()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();
        SystemAccessControl opaAuthorizer = factory.create(Map.of("opa.policy.uri", "foo", "opa.policy.batched-uri", "bar"));

        assertInstanceOf(OpaBatchAccessControl.class, opaAuthorizer);
        assertInstanceOf(OpaAccessControl.class, opaAuthorizer);
    }

    @Test
    public void testBasePolicyUriCannotBeUnset()
    {
        OpaAccessControlFactory factory = new OpaAccessControlFactory();

        assertThrows(
                ApplicationConfigurationException.class,
                () -> factory.create(Map.of()),
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
