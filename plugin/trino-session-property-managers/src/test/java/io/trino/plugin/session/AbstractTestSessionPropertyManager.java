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
package io.trino.plugin.session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.session.SessionConfigurationContext;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

public abstract class AbstractTestSessionPropertyManager
{
    protected static final SessionConfigurationContext CONTEXT = new SessionConfigurationContext(
            "user",
            Optional.of("source"),
            ImmutableSet.of("tag1", "tag2"),
            Optional.of(QueryType.DATA_DEFINITION.toString()),
            new ResourceGroupId(ImmutableList.of("global", "pipeline", "user_foo", "bar")));

    protected abstract void assertProperties(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties, SessionMatchSpec... spec)
            throws Exception;

    @Test
    public void testResourceGroupMatch()
            throws Exception
    {
        Map<String, String> systemProperties = ImmutableMap.of("PROPERTY1", "VALUE1", "PROPERTY2", "VALUE2");
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(Pattern.compile("global.pipeline.user_.*")),
                systemProperties);

        assertProperties(systemProperties, ImmutableMap.of(), spec);
    }

    @Test
    public void testClientTagMatch()
            throws Exception
    {
        ImmutableMap<String, String> systemProperties = ImmutableMap.of("PROPERTY", "VALUE");
        SessionMatchSpec spec = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag2")),
                Optional.empty(),
                Optional.empty(),
                systemProperties);

        assertProperties(systemProperties, ImmutableMap.of(), spec);
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
                ImmutableMap.of(
                        "PROPERTY1", "VALUE1",
                        "PROPERTY3", "VALUE3",
                        "CATALOG1.PROPERTY1", "VALUE_C1_P1",
                        "CATALOG1.PROPERTY3", "VALUE_C1_P3",
                        "CATALOG2.PROPERTY", "VALUE_C2"));
        SessionMatchSpec spec2 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag1", "tag2")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(
                        "PROPERTY2", "VALUE2",
                        "CATALOG1.PROPERTY2", "VALUE_C1_P2",
                        "CATALOG3.PROPERTY", "VALUE_C3"));

        assertProperties(
                ImmutableMap.of(
                        "PROPERTY1", "VALUE1",
                        "PROPERTY2", "VALUE2",
                        "PROPERTY3", "VALUE3"),
                ImmutableMap.of(
                        "CATALOG1", ImmutableMap.of(
                                "PROPERTY1", "VALUE_C1_P1",
                                "PROPERTY2", "VALUE_C1_P2",
                                "PROPERTY3", "VALUE_C1_P3"),
                        "CATALOG2", ImmutableMap.of("PROPERTY", "VALUE_C2"),
                        "CATALOG3", ImmutableMap.of("PROPERTY", "VALUE_C3")),
                spec1, spec2);
    }

    @Test
    public void testSystemPropertyOverrides()
            throws Exception
    {
        SessionMatchSpec spec1 = matchAllSpec(ImmutableMap.of(
                "PROPERTY0", "VALUE0",
                "PROPERTY1", "VALUE1",
                "PROPERTY2", "VALUE2",
                "PROPERTY3", "VALUE3"));
        SessionMatchSpec spec2 = matchAllSpec(ImmutableMap.of(
                "PROPERTY2", "VALUE2_BIS",
                "PROPERTY3", "VALUE3_BIS"));
        SessionMatchSpec spec3 = matchAllSpec(ImmutableMap.of(
                "PROPERTY0", "VALUE0_TER",
                "PROPERTY3", "VALUE3_TER"));

        assertProperties(
                ImmutableMap.of(
                        "PROPERTY0", "VALUE0_TER",
                        "PROPERTY1", "VALUE1",
                        "PROPERTY2", "VALUE2_BIS",
                        "PROPERTY3", "VALUE3_TER"),
                ImmutableMap.of(),
                spec1, spec2, spec3);
    }

    @Test
    public void testCatalogPropertyOverrides()
            throws Exception
    {
        SessionMatchSpec spec1 = matchAllSpec(ImmutableMap.of(
                "CATALOG.PROPERTY0", "VALUE0",
                "CATALOG.PROPERTY1", "VALUE1",
                "CATALOG.PROPERTY2", "VALUE2",
                "CATALOG.PROPERTY3", "VALUE3"));
        SessionMatchSpec spec2 = matchAllSpec(ImmutableMap.of(
                "CATALOG.PROPERTY2", "VALUE2_BIS",
                "CATALOG.PROPERTY3", "VALUE3_BIS"));
        SessionMatchSpec spec3 = matchAllSpec(ImmutableMap.of(
                "CATALOG.PROPERTY0", "VALUE0_TER",
                "CATALOG.PROPERTY3", "VALUE3_TER"));

        assertProperties(
                ImmutableMap.of(),
                ImmutableMap.of("CATALOG", ImmutableMap.of(
                        "PROPERTY0", "VALUE0_TER",
                        "PROPERTY1", "VALUE1",
                        "PROPERTY2", "VALUE2_BIS",
                        "PROPERTY3", "VALUE3_TER")),
                spec1, spec2, spec3);
    }

    @Test
    public void testCatalogPropertiesWithDots()
            throws Exception
    {
        SessionMatchSpec spec1 = new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.of(ImmutableList.of("tag2")),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of("CATALOG.P.R.O.P.E.R.T.Y", "VALUE"));
        assertProperties(
                ImmutableMap.of(),
                ImmutableMap.of("CATALOG", ImmutableMap.of("P.R.O.P.E.R.T.Y", "VALUE")),
                spec1);
    }

    @Test
    public void testEmptyPropertyAndCatalogNames()
            throws Exception
    {
        SessionMatchSpec spec1 = matchAllSpec(ImmutableMap.of(
                "", "VALUE1",
                "CATALOG1.", "VALUE2",
                ".PROPERTY", "VALUE3",
                ".", "VALUE4"));
        assertProperties(
                ImmutableMap.of("", "VALUE1"),
                ImmutableMap.of(
                        "CATALOG1", ImmutableMap.of("", "VALUE2"),
                        "", ImmutableMap.of(
                                "PROPERTY", "VALUE3",
                                "", "VALUE4")),
                spec1);
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
                ImmutableMap.of("PROPERTY", "VALUE"));

        assertProperties(ImmutableMap.of(), ImmutableMap.of(), spec);
    }

    private SessionMatchSpec matchAllSpec(Map<String, String> sessionProperties)
    {
        return new SessionMatchSpec(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                sessionProperties);
    }
}
