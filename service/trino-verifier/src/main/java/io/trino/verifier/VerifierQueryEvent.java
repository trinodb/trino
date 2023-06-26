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
package io.trino.verifier;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;

import java.util.List;

@Immutable
@EventType("VerifierQuery")
public class VerifierQueryEvent
{
    private final String suite;
    private final String runId;
    private final String source;
    private final String name;
    private final boolean failed;

    private final String testCatalog;
    private final String testSchema;
    private final List<String> testSetupQueries;
    private final String testQuery;
    private final List<String> testTeardownQueries;
    private final List<String> testSetupQueryIds;
    private final String testQueryId;
    private final List<String> testTeardownQueryIds;
    private final Double testCpuTimeSecs;
    private final Double testWallTimeSecs;

    private final String controlCatalog;
    private final String controlSchema;
    private final List<String> controlSetupQueries;
    private final String controlQuery;
    private final List<String> controlTeardownQueries;
    private final List<String> controlSetupQueryIds;
    private final String controlQueryId;
    private final List<String> controlTeardownQueryIds;
    private final Double controlCpuTimeSecs;
    private final Double controlWallTimeSecs;

    private final String errorMessage;

    public VerifierQueryEvent(
            String suite,
            String runId,
            String source,
            String name,
            boolean failed,
            String testCatalog,
            String testSchema,
            List<String> testSetupQueries,
            String testQuery,
            List<String> testTeardownQueries,
            List<String> testSetupQueryIds,
            String testQueryId,
            List<String> testTeardownQueryIds,
            Double testCpuTimeSecs,
            Double testWallTimeSecs,
            String controlCatalog,
            String controlSchema,
            List<String> controlSetupQueries,
            String controlQuery,
            List<String> controlTeardownQueries,
            List<String> controlSetupQueryIds,
            String controlQueryId,
            List<String> controlTeardownQueryIds,
            Double controlCpuTimeSecs,
            Double controlWallTimeSecs,
            String errorMessage)
    {
        this.suite = suite;
        this.runId = runId;
        this.source = source;
        this.name = name;
        this.failed = failed;

        this.testCatalog = testCatalog;
        this.testSchema = testSchema;
        this.testSetupQueries = ImmutableList.copyOf(testSetupQueries);
        this.testQuery = testQuery;
        this.testTeardownQueries = ImmutableList.copyOf(testTeardownQueries);
        this.testSetupQueryIds = ImmutableList.copyOf(testSetupQueryIds);
        this.testQueryId = testQueryId;
        this.testTeardownQueryIds = ImmutableList.copyOf(testTeardownQueryIds);
        this.testCpuTimeSecs = testCpuTimeSecs;
        this.testWallTimeSecs = testWallTimeSecs;

        this.controlCatalog = controlCatalog;
        this.controlSchema = controlSchema;
        this.controlSetupQueries = ImmutableList.copyOf(controlSetupQueries);
        this.controlQuery = controlQuery;
        this.controlTeardownQueries = ImmutableList.copyOf(controlTeardownQueries);
        this.controlSetupQueryIds = ImmutableList.copyOf(controlSetupQueryIds);
        this.controlQueryId = controlQueryId;
        this.controlTeardownQueryIds = ImmutableList.copyOf(controlTeardownQueryIds);
        this.controlCpuTimeSecs = controlCpuTimeSecs;
        this.controlWallTimeSecs = controlWallTimeSecs;

        this.errorMessage = errorMessage;
    }

    @EventField
    public String getSuite()
    {
        return suite;
    }

    @EventField
    public String getRunId()
    {
        return runId;
    }

    @EventField
    public String getSource()
    {
        return source;
    }

    @EventField
    public String getName()
    {
        return name;
    }

    @EventField
    public boolean isFailed()
    {
        return failed;
    }

    @EventField
    public String getTestCatalog()
    {
        return testCatalog;
    }

    @EventField
    public String getTestSchema()
    {
        return testSchema;
    }

    @EventField
    public String getTestQuery()
    {
        return testQuery;
    }

    @EventField
    public List<String> getTestSetupQueryIds()
    {
        return testSetupQueryIds;
    }

    @EventField
    public String getTestQueryId()
    {
        return testQueryId;
    }

    @EventField
    public List<String> getTestTeardownQueryIds()
    {
        return testTeardownQueryIds;
    }

    @EventField
    public Double getTestCpuTimeSecs()
    {
        return testCpuTimeSecs;
    }

    @EventField
    public Double getTestWallTimeSecs()
    {
        return testWallTimeSecs;
    }

    @EventField
    public String getControlCatalog()
    {
        return controlCatalog;
    }

    @EventField
    public String getControlSchema()
    {
        return controlSchema;
    }

    @EventField
    public String getControlQuery()
    {
        return controlQuery;
    }

    @EventField
    public List<String> getControlSetupQueryIds()
    {
        return controlSetupQueryIds;
    }

    @EventField
    public String getControlQueryId()
    {
        return controlQueryId;
    }

    @EventField
    public List<String> getControlTeardownQueryIds()
    {
        return controlTeardownQueryIds;
    }

    @EventField
    public Double getControlCpuTimeSecs()
    {
        return controlCpuTimeSecs;
    }

    @EventField
    public Double getControlWallTimeSecs()
    {
        return controlWallTimeSecs;
    }

    @EventField
    public String getErrorMessage()
    {
        return errorMessage;
    }

    @EventField
    public List<String> getTestSetupQueries()
    {
        return testSetupQueries;
    }

    @EventField
    public List<String> getTestTeardownQueries()
    {
        return testTeardownQueries;
    }

    @EventField
    public List<String> getControlSetupQueries()
    {
        return controlSetupQueries;
    }

    @EventField
    public List<String> getControlTeardownQueries()
    {
        return controlTeardownQueries;
    }
}
