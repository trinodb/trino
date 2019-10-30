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
package io.prestosql.plugin.session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.session.SessionConfigurationContext;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.regex.Pattern;

@Test
public abstract class AbstractTestSessionPropertyManager
{
    protected static final SessionConfigurationContext CONTEXT = new SessionConfigurationContext(
            "user",
            Optional.of("source"),
            ImmutableSet.of("tag1", "tag2"),
            Optional.of(QueryType.DATA_DEFINITION.toString()),
            new ResourceGroupId(ImmutableList.of("global", "pipeline", "user_foo", "bar")));

    protected abstract void assertProperties(SessionProperties sessionProperties, SessionMatchSpec... spec)
            throws Exception;

    @Test
    public void testResourceGroupMatch()
            throws Exception
    {
        SessionProperties sessionProperties = new SessionProperties(
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2"),
                ImmutableMap.of());
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("global.pipeline.user_.*")),
                sessionProperties.getSystemProperties(),
                sessionProperties.getCatalogsProperties());

        assertProperties(sessionProperties, spec);
    }

    @Test
    public void testClientTagMatch()
            throws Exception
    {
        SessionProperties sessionProperties = new SessionProperties(
                ImmutableMap.of("PROPERTY", "VALUE"),
                ImmutableMap.of());
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag2")),
                Optional.empty(),
                Optional.empty(),
                sessionProperties.getSystemProperties(),
                sessionProperties.getCatalogsProperties());

        assertProperties(sessionProperties, spec);
    }

    @Test
    public void testMultipleMatch()
            throws Exception
    {
        SessionMatchSpec spec1 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag2")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY3", "VALUE3"),
                ImmutableMap.of(
                        "CATALOG1", ImmutableMap.of("PROPERTY_C1_P1", "VALUE_C1_P1"),
                        "CATALOG2", ImmutableMap.of("PROPERTY_C2_P1", "VALUE_C2_P1")));
        SessionMatchSpec spec2 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2"),
                ImmutableMap.of(
                        "CATALOG1", ImmutableMap.of("PROPERTY_C1_P2", "VALUE_C1_P2"),
                        "CATALOG2", ImmutableMap.of()));
        SessionProperties expectedSessionProperties = new SessionProperties(
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2", "PROPERTY3", "VALUE3"),
                ImmutableMap.of(
                        "CATALOG1", ImmutableMap.of("PROPERTY_C1_P1", "VALUE_C1_P1", "PROPERTY_C1_P2", "VALUE_C1_P2"),
                        "CATALOG2", ImmutableMap.of("PROPERTY_C2_P1", "VALUE_C2_P1")));

        assertProperties(expectedSessionProperties, spec1, spec2);
    }

    @Test
    public void testOverrides()
            throws Exception
    {
        SessionMatchSpec spec1 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag2")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY3", "VALUE3"),
                ImmutableMap.of("CATALOG1", ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2")));
        SessionMatchSpec spec2 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY1", "VALUE1_BIS", "PROPERTY2", "VALUE2"),
                ImmutableMap.of("CATALOG1", ImmutableMap.of("PROPERTY1", "VALUE1_BIS", "PROPERTY2", "VALUE2_BIS")));
        SessionMatchSpec spec3 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("PROPERTY2", "VALUE2_TER", "PROPERTY3", "VALUE3_TER"),
                ImmutableMap.of("CATALOG1", ImmutableMap.of("PROPERTY2", "VALUE2_TER")));
        SessionProperties expectedSessionProperties = new SessionProperties(
                ImmutableMap.of("PROPERTY1", "VALUE1_BIS", "PROPERTY2", "VALUE2_TER", "PROPERTY3", "VALUE3_TER"),
                ImmutableMap.of("CATALOG1", ImmutableMap.of("PROPERTY1", "VALUE1_BIS", "PROPERTY2", "VALUE2_TER")));

        assertProperties(expectedSessionProperties, spec1, spec2, spec3);
    }

    @Test
    public void testNoMatch()
            throws Exception
    {
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("global.interactive.user_.*")),
                ImmutableMap.of("PROPERTY", "VALUE"),
                ImmutableMap.of("CATALOG1", ImmutableMap.of("PROPERTY_C1", "VALUE_C1")));

        assertProperties(SessionProperties.EMPTY, spec);
    }
}
