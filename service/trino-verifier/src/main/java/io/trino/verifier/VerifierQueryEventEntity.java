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

import java.util.Optional;
import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;

public class VerifierQueryEventEntity
{
    private final String suite;
    private final Optional<String> runId;
    private final Optional<String> source;
    private final Optional<String> name;
    private final boolean failed;

    private final Optional<String> testCatalog;
    private final Optional<String> testSchema;
    private final Optional<String> testSetupQueryIdsJson;
    private final Optional<String> testQueryId;
    private final Optional<String> testTeardownQueryIdsJson;
    private final OptionalDouble testCpuTimeSeconds;
    private final OptionalDouble testWallTimeSeconds;

    private final Optional<String> controlCatalog;
    private final Optional<String> controlSchema;
    private final Optional<String> controlSetupQueryIdsJson;
    private final Optional<String> controlQueryId;
    private final Optional<String> controlTeardownQueryIdsJson;
    private final OptionalDouble controlCpuTimeSeconds;
    private final OptionalDouble controlWallTimeSeconds;

    private final Optional<String> errorMessage;

    public VerifierQueryEventEntity(
            String suite,
            Optional<String> runId,
            Optional<String> source,
            Optional<String> name,
            boolean failed,
            Optional<String> testCatalog,
            Optional<String> testSchema,
            Optional<String> testSetupQueryIdsJson,
            Optional<String> testQueryId,
            Optional<String> testTeardownQueryIdsJson,
            OptionalDouble testCpuTimeSeconds,
            OptionalDouble testWallTimeSeconds,
            Optional<String> controlCatalog,
            Optional<String> controlSchema,
            Optional<String> controlSetupQueryIdsJson,
            Optional<String> controlQueryId,
            Optional<String> controlTeardownQueryIdsJson,
            OptionalDouble controlCpuTimeSeconds,
            OptionalDouble controlWallTimeSeconds,
            Optional<String> errorMessage)
    {
        this.suite = requireNonNull(suite, "suite is null");
        this.runId = requireNonNull(runId, "runId is null");
        this.source = requireNonNull(source, "source is null");
        this.name = requireNonNull(name, "name is null");
        this.failed = failed;
        this.testCatalog = requireNonNull(testCatalog, "testCatalog is null");
        this.testSchema = requireNonNull(testSchema, "testSchema is null");
        this.testSetupQueryIdsJson = requireNonNull(testSetupQueryIdsJson, "testSetupQueryIdsJson is null");
        this.testQueryId = requireNonNull(testQueryId, "testQueryId is null");
        this.testTeardownQueryIdsJson = requireNonNull(testTeardownQueryIdsJson, "testTeardownQueryIdsJson is null");
        this.testCpuTimeSeconds = requireNonNull(testCpuTimeSeconds, "testCpuTimeSeconds is null");
        this.testWallTimeSeconds = requireNonNull(testWallTimeSeconds, "testWallTimeSeconds is null");
        this.controlCatalog = requireNonNull(controlCatalog, "controlCatalog is null");
        this.controlSchema = requireNonNull(controlSchema, "controlSchema is null");
        this.controlSetupQueryIdsJson = requireNonNull(controlSetupQueryIdsJson, "controlSetupQueryIdsJson is null");
        this.controlQueryId = requireNonNull(controlQueryId, "controlQueryId is null");
        this.controlTeardownQueryIdsJson = requireNonNull(controlTeardownQueryIdsJson, "controlTeardownQueryIdsJson is null");
        this.controlCpuTimeSeconds = requireNonNull(controlCpuTimeSeconds, "controlCpuTimeSeconds is null");
        this.controlWallTimeSeconds = requireNonNull(controlWallTimeSeconds, "controlWallTimeSeconds is null");
        this.errorMessage = requireNonNull(errorMessage, "errorMessage is null");
    }

    public String getSuite()
    {
        return suite;
    }

    public Optional<String> getRunId()
    {
        return runId;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getName()
    {
        return name;
    }

    public boolean isFailed()
    {
        return failed;
    }

    public Optional<String> getTestCatalog()
    {
        return testCatalog;
    }

    public Optional<String> getTestSchema()
    {
        return testSchema;
    }

    public Optional<String> getTestSetupQueryIdsJson()
    {
        return testSetupQueryIdsJson;
    }

    public Optional<String> getTestQueryId()
    {
        return testQueryId;
    }

    public Optional<String> getTestTeardownQueryIdsJson()
    {
        return testTeardownQueryIdsJson;
    }

    public OptionalDouble getTestCpuTimeSeconds()
    {
        return testCpuTimeSeconds;
    }

    public OptionalDouble getTestWallTimeSeconds()
    {
        return testWallTimeSeconds;
    }

    public Optional<String> getControlCatalog()
    {
        return controlCatalog;
    }

    public Optional<String> getControlSchema()
    {
        return controlSchema;
    }

    public Optional<String> getControlSetupQueryIdsJson()
    {
        return controlSetupQueryIdsJson;
    }

    public Optional<String> getControlQueryId()
    {
        return controlQueryId;
    }

    public Optional<String> getControlTeardownQueryIdsJson()
    {
        return controlTeardownQueryIdsJson;
    }

    public OptionalDouble getControlCpuTimeSeconds()
    {
        return controlCpuTimeSeconds;
    }

    public OptionalDouble getControlWallTimeSeconds()
    {
        return controlWallTimeSeconds;
    }

    public Optional<String> getErrorMessage()
    {
        return errorMessage;
    }
}
