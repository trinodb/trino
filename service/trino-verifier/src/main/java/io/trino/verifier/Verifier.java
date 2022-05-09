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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.concat;
import static io.trino.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static io.trino.spi.StandardErrorCode.TOO_MANY_REQUESTS_FAILED;
import static io.trino.verifier.QueryResult.State.SUCCESS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectories;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Verifier
{
    private static final Logger log = Logger.get(Verifier.class);

    private static final Set<ErrorCode> EXPECTED_ERRORS = ImmutableSet.<ErrorCode>builder()
            .add(REMOTE_TASK_MISMATCH.toErrorCode())
            .add(TOO_MANY_REQUESTS_FAILED.toErrorCode())
            .add(PAGE_TRANSPORT_TIMEOUT.toErrorCode())
            .build();

    private final VerifierConfig config;
    private final Set<EventClient> eventClients;
    private final int threadCount;
    private final Set<String> allowedQueries;
    private final Set<String> bannedQueries;
    private final int precision;

    public Verifier(PrintStream out, VerifierConfig config, Set<EventClient> eventClients)
    {
        requireNonNull(out, "out is null");
        this.config = requireNonNull(config, "config is null");
        this.eventClients = requireNonNull(eventClients, "eventClients is null");
        this.allowedQueries = requireNonNull(config.getAllowedQueries(), "allowedQueries is null");
        this.bannedQueries = requireNonNull(config.getBannedQueries(), "bannedQueries is null");
        this.threadCount = config.getThreadCount();
        this.precision = config.getDoublePrecision();
    }

    // Returns number of failed queries
    public int run(List<QueryPair> queries)
            throws InterruptedException
    {
        ExecutorService executor = newFixedThreadPool(threadCount);
        CompletionService<Validator> completionService = new ExecutorCompletionService<>(executor);

        int totalQueries = queries.size() * config.getSuiteRepetitions() * config.getQueryRepetitions();
        log.info("Total Queries:     %d", totalQueries);

        log.info("Allowed Queries: %s", Joiner.on(',').join(allowedQueries));

        int queriesSubmitted = 0;
        for (int i = 0; i < config.getSuiteRepetitions(); i++) {
            for (QueryPair query : queries) {
                for (int j = 0; j < config.getQueryRepetitions(); j++) {
                    // If we have allowed queries, only run the tests on those
                    if (!allowedQueries.isEmpty() && !allowedQueries.contains(query.getName())) {
                        log.debug("Query %s is not allowed", query.getName());
                        continue;
                    }
                    if (bannedQueries.contains(query.getName())) {
                        log.debug("Query %s is banned", query.getName());
                        continue;
                    }
                    Validator validator = new Validator(
                            config.getControlGateway(),
                            config.getTestGateway(),
                            config.getControlTimeout(),
                            config.getTestTimeout(),
                            config.getMaxRowCount(),
                            config.isExplainOnly(),
                            config.getDoublePrecision(),
                            isCheckCorrectness(query),
                            config.isCheckDeterminismEnabled(),
                            config.isVerboseResultsComparison(),
                            config.getControlTeardownRetries(),
                            config.getTestTeardownRetries(),
                            config.getRunTearDownOnResultMismatch(),
                            config.isSkipControl(),
                            query);
                    completionService.submit(validator::valid, validator);
                    queriesSubmitted++;
                }
            }
        }

        log.info("Allowed Queries:     %d", queriesSubmitted);
        log.info("Skipped Queries:     %d", (totalQueries - queriesSubmitted));
        log.info("---------------------");

        executor.shutdown();

        int total = 0;
        int valid = 0;
        int failed = 0;
        int skipped = 0;
        double lastProgress = 0;

        while (total < queriesSubmitted) {
            total++;

            Validator validator = takeUnchecked(completionService);

            if (validator.isSkipped()) {
                if (!config.isQuiet()) {
                    log.warn("%s", validator.getSkippedMessage());
                }

                skipped++;
                continue;
            }

            QueryResult controlResult = validator.getControlResult();
            if (config.isSimplifiedControlQueriesGenerationEnabled() && controlResult.getState() == SUCCESS) {
                QueryPair queryPair = validator.getQueryPair();
                Path path = Paths.get(format(
                        "%s/%s/%s/%s.sql",
                        config.getSimplifiedControlQueriesOutputDirectory(),
                        config.getRunId(),
                        queryPair.getSuite(),
                        queryPair.getName()));
                try {
                    String content = generateCorrespondingSelect(controlResult.getColumnTypes(), controlResult.getResults());
                    createDirectories(path.getParent());
                    Files.write(path, content.getBytes(UTF_8));
                }
                catch (IOException | RuntimeException e) {
                    log.error(e, "Failed generating corresponding select statement for expected results for query %s", queryPair.getName());
                }
            }

            if (validator.valid()) {
                valid++;
            }
            else {
                failed++;
            }

            for (EventClient eventClient : eventClients) {
                eventClient.post(buildEvent(validator));
            }

            double progress = (((double) total) / totalQueries) * 100;
            if (!config.isQuiet() || (progress - lastProgress) > 1) {
                log.info("Progress: %s valid, %s failed, %s skipped, %.2f%% done", valid, failed, skipped, progress);
                lastProgress = progress;
            }
        }

        log.info("Results: %s / %s (%s skipped)", valid, failed, skipped);
        log.info("");

        for (EventClient eventClient : eventClients) {
            if (eventClient instanceof Closeable) {
                try {
                    ((Closeable) eventClient).close();
                }
                catch (IOException ignored) {
                }
                log.info("");
            }
        }

        return failed;
    }

    private boolean isCheckCorrectness(QueryPair query)
    {
        // Check if either the control query or the test query matches the regex
        if (Pattern.matches(config.getSkipCorrectnessRegex(), query.getTest().getQuery()) ||
                Pattern.matches(config.getSkipCorrectnessRegex(), query.getControl().getQuery())) {
            // If so disable correctness checking
            return false;
        }
        else {
            return config.isCheckCorrectnessEnabled();
        }
    }

    private VerifierQueryEvent buildEvent(Validator validator)
    {
        String errorMessage = null;
        QueryPair queryPair = validator.getQueryPair();
        QueryResult control = validator.getControlResult();
        QueryResult test = validator.getTestResult();

        if (!validator.valid()) {
            errorMessage = format("Test state %s, Control state %s\n", test.getState(), control.getState());
            Exception e = test.getException();
            if (e != null && shouldAddStackTrace(e)) {
                errorMessage += getStackTraceAsString(e);
            }
            if (control.getState() == SUCCESS && test.getState() == SUCCESS) {
                errorMessage += validator.getResultsComparison(precision).trim();
            }
        }

        return new VerifierQueryEvent(
                queryPair.getSuite(),
                config.getRunId(),
                config.getSource(),
                queryPair.getName(),
                !validator.valid(),
                queryPair.getTest().getCatalog(),
                queryPair.getTest().getSchema(),
                queryPair.getTest().getPreQueries(),
                queryPair.getTest().getQuery(),
                queryPair.getTest().getPostQueries(),
                validator.getTestPreQueryResults().stream()
                        .map(QueryResult::getQueryId)
                        .filter(Objects::nonNull)
                        .collect(toImmutableList()),
                test.getQueryId(),
                validator.getTestPostQueryResults().stream()
                        .map(QueryResult::getQueryId)
                        .filter(Objects::nonNull)
                        .collect(toImmutableList()),
                getTotalDurationInSeconds(validator.getTestPreQueryResults(), validator.getTestResult(), validator.getTestPostQueryResults(), QueryResult::getCpuTime),
                getTotalDurationInSeconds(validator.getTestPreQueryResults(), validator.getTestResult(), validator.getTestPostQueryResults(), QueryResult::getWallTime),
                queryPair.getControl().getCatalog(),
                queryPair.getControl().getSchema(),
                queryPair.getControl().getPreQueries(),
                queryPair.getControl().getQuery(),
                queryPair.getControl().getPostQueries(),
                validator.getControlPreQueryResults().stream()
                        .map(QueryResult::getQueryId)
                        .filter(Objects::nonNull)
                        .collect(toImmutableList()),
                control.getQueryId(),
                validator.getControlPostQueryResults().stream()
                        .map(QueryResult::getQueryId)
                        .filter(Objects::nonNull)
                        .collect(toImmutableList()),
                getTotalDurationInSeconds(validator.getControlPreQueryResults(), validator.getControlResult(), validator.getControlPostQueryResults(), QueryResult::getCpuTime),
                getTotalDurationInSeconds(validator.getControlPreQueryResults(), validator.getControlResult(), validator.getControlPostQueryResults(), QueryResult::getWallTime),
                errorMessage);
    }

    @Nullable
    private static Double getTotalDurationInSeconds(List<QueryResult> preQueries, QueryResult query, List<QueryResult> postQueries, Function<QueryResult, Duration> metric)
    {
        OptionalDouble result = concat(preQueries.stream(), Stream.of(query), postQueries.stream())
                .map(metric)
                .filter(Objects::nonNull)
                .mapToDouble(duration -> duration.getValue(SECONDS))
                .reduce(Double::sum);
        if (result.isEmpty()) {
            return null;
        }
        return result.getAsDouble();
    }

    private static <T> T takeUnchecked(CompletionService<T> completionService)
            throws InterruptedException
    {
        try {
            return completionService.take().get();
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean shouldAddStackTrace(Exception e)
    {
        if (e instanceof TrinoException) {
            ErrorCode errorCode = ((TrinoException) e).getErrorCode();
            if (EXPECTED_ERRORS.contains(errorCode)) {
                return false;
            }
        }
        return true;
    }

    private static String generateCorrespondingSelect(List<String> columnTypes, List<List<Object>> rows)
    {
        StringBuilder sb = new StringBuilder("SELECT *\nFROM\n(\n  VALUES\n");
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            List<Object> row = rows.get(rowIndex);
            sb.append("    (");
            for (int columnIndex = 0; columnIndex < columnTypes.size(); columnIndex++) {
                String type = columnTypes.get(columnIndex);
                Optional<String> value = Optional.ofNullable(row.get(columnIndex)).map(Object::toString);
                String literal = getLiteral(type, value);
                sb.append(literal);
                if (columnIndex < columnTypes.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")");
            if (rowIndex < rows.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        if (rows.isEmpty()) {
            sb.append("    (");
            for (int columnIndex = 0; columnIndex < columnTypes.size(); columnIndex++) {
                sb.append("NULL");
                if (columnIndex < columnTypes.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")\n");
        }
        sb.append(")\n");
        if (rows.isEmpty()) {
            sb.append("WHERE 1=0\n");
        }
        return sb.toString();
    }

    private static String getLiteral(String type, Optional<String> value)
    {
        String baseType = getBaseType(type);
        switch (baseType) {
            case "TINYINT":
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":
            case "DECIMAL":
            case "DATE":
            case "TIME":
            case "REAL":
            case "DOUBLE":
                return value.map(v -> baseType + " '" + v + "'").orElse("NULL");
            case "CHAR":
            case "VARCHAR":
                return value.map(v -> baseType + " '" + v.replaceAll("'", "''") + "'").orElse("NULL");
            case "VARBINARY":
                return value.map(v -> "X'" + v + "'").orElse("NULL");
            case "UNKNOWN":
                return "NULL";
            default:
                throw new IllegalArgumentException(format("Unexpected type: %s", type));
        }
    }

    private static String getBaseType(String type)
    {
        String baseType = type.toUpperCase(ENGLISH);
        int index = baseType.indexOf('(');
        if (index != -1) {
            baseType = baseType.substring(0, index);
        }
        return baseType;
    }
}
