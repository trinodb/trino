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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.SessionPropertyManager;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.server.protocol.Slug;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.session.ResourceEstimates;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.SqlPath;
import io.trino.transaction.TransactionId;
import io.trino.transaction.TransactionManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class AutoAnalyzeService
{
    private static final Logger log = Logger.get(AutoAnalyzeService.class);

    // Internal username for ANALYZE query sessions. Cluster operators may need to allow
    // this user in their access control configuration.
    private static final String AUTO_ANALYZE_USER = "$auto-analyze";
    private static final String INFORMATION_SCHEMA = "information_schema";

    private final AutoAnalyzeConfig config;
    private final Metadata metadata;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final DispatchManager dispatchManager;
    private final QueryIdGenerator queryIdGenerator;

    private final ConcurrentHashMap<QualifiedObjectName, Instant> lastAnalyzed = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("auto-analyze-%s"));

    private ScheduledFuture<?> scheduledFuture;

    @Inject
    public AutoAnalyzeService(
            AutoAnalyzeConfig config,
            Metadata metadata,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionPropertyManager sessionPropertyManager,
            DispatchManager dispatchManager,
            QueryIdGenerator queryIdGenerator)
    {
        this.config = requireNonNull(config, "config is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
    }

    @PostConstruct
    public void start()
    {
        if (!config.isEnabled()) {
            return;
        }
        long intervalMillis = config.getCheckInterval().toMillis();
        scheduledFuture = executor.scheduleWithFixedDelay(() -> {
            try {
                checkAndAnalyze();
            }
            catch (Throwable e) {
                log.error(e, "Error during auto-analyze check");
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
            scheduledFuture = null;
        }
        executor.shutdownNow();
    }

    private void checkAndAnalyze()
    {
        List<QualifiedObjectName> tablesToAnalyze = collectStaleTables();
        for (QualifiedObjectName table : tablesToAnalyze) {
            submitAnalyze(table);
        }
    }

    private List<QualifiedObjectName> collectStaleTables()
    {
        // Run catalog/table listing and statistics reads inside a single auto-commit transaction.
        TransactionId transactionId = transactionManager.beginTransaction(true);
        try {
            Session browseSession = createBrowseSession(transactionId);
            List<QualifiedObjectName> stale = findStaleTables(browseSession);
            transactionManager.asyncCommit(transactionId);
            return stale;
        }
        catch (Throwable e) {
            log.error(e, "Failed to collect tables for auto-analyze");
            transactionManager.asyncAbort(transactionId);
            return ImmutableList.of();
        }
    }

    private List<QualifiedObjectName> findStaleTables(Session browseSession)
    {
        List<QualifiedObjectName> stale = new ArrayList<>();
        for (CatalogInfo catalogInfo : metadata.listCatalogs(browseSession)) {
            String catalogName = catalogInfo.catalogName();
            List<QualifiedObjectName> tables;
            try {
                tables = metadata.listTables(browseSession, new QualifiedTablePrefix(catalogName));
            }
            catch (Throwable e) {
                log.warn(e, "Failed to list tables in catalog %s", catalogName);
                continue;
            }
            for (QualifiedObjectName table : tables) {
                if (table.schemaName().equalsIgnoreCase(INFORMATION_SCHEMA)) {
                    continue;
                }
                if (isStale(browseSession, table)) {
                    stale.add(table);
                }
            }
        }
        return stale;
    }

    private boolean isStale(Session browseSession, QualifiedObjectName table)
    {
        Instant lastTime = lastAnalyzed.get(table);
        if (lastTime == null) {
            // Never analyzed — treat as stale immediately
            return true;
        }

        long elapsedMillis = Instant.now().toEpochMilli() - lastTime.toEpochMilli();
        long intervalMillis = resolveIntervalMillis(browseSession, table);
        return elapsedMillis >= intervalMillis;
    }

    private long resolveIntervalMillis(Session browseSession, QualifiedObjectName table)
    {
        try {
            Optional<TableHandle> tableHandle = metadata.getTableHandle(browseSession, table);
            if (tableHandle.isPresent()) {
                TableStatistics stats = metadata.getTableStatistics(browseSession, tableHandle.get());
                Estimate rowCount = stats.getRowCount();
                if (!rowCount.isUnknown() && rowCount.getValue() >= config.getSmallTableThreshold()) {
                    return config.getLargeTableInterval().toMillis();
                }
            }
        }
        catch (Throwable e) {
            log.debug(e, "Could not fetch statistics for %s; defaulting to small-table interval", table);
        }
        return config.getSmallTableInterval().toMillis();
    }

    private void submitAnalyze(QualifiedObjectName table)
    {
        // Record submission time before dispatching so that a slow ANALYZE does not
        // cause repeated re-submission on subsequent check cycles.
        lastAnalyzed.put(table, Instant.now());

        QueryId queryId = dispatchManager.createQueryId();
        String analyzeQuery = format(
                "ANALYZE \"%s\".\"%s\".\"%s\"",
                table.catalogName().replace("\"", "\"\""),
                table.schemaName().replace("\"", "\"\""),
                table.objectName().replace("\"", "\"\""));

        try {
            dispatchManager.createQuery(queryId, Span.getInvalid(), Slug.createNew(), createAnalyzeSessionContext(), analyzeQuery);
            log.debug("Submitted auto-analyze for %s (queryId=%s)", table, queryId);
        }
        catch (Throwable e) {
            log.warn(e, "Failed to submit auto-analyze for %s", table);
        }
    }

    private Session createBrowseSession(TransactionId transactionId)
    {
        Identity identity = Identity.ofUser(AUTO_ANALYZE_USER);
        QueryId queryId = queryIdGenerator.createNextQueryId();
        Session session = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setQuerySpan(Span.getInvalid())
                .setIdentity(identity)
                .setOriginalIdentity(identity)
                .setSource("auto-analyze")
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .setLocale(Locale.ENGLISH)
                .setPath(SqlPath.EMPTY_PATH)
                .build();
        return session.beginTransactionId(transactionId, transactionManager, accessControl);
    }

    private SessionContext createAnalyzeSessionContext()
    {
        Identity identity = Identity.ofUser(AUTO_ANALYZE_USER);
        return new SessionContext(
                TRINO_HEADERS,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                identity,
                identity,
                new SelectedRole(SelectedRole.Type.NONE, Optional.empty()),
                Optional.of("auto-analyze"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty(),
                false,
                Optional.empty(),
                Optional.empty());
    }
}
