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
package io.trino.plugin.paimon;

import io.airlift.units.Duration;
import io.trino.plugin.paimon.catalog.PaimonCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.paimon.PaimonErrorCode.PAIMON_CANNOT_OPEN_SPLIT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.DynamicFilter.NOT_BLOCKED;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.paimon.options.Options.fromMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DynamicFilteringTrinoSplitSourceTest
{
    @Test
    public void testNonAwaitableDynamicPredicateIsNotPushedBySplitManager()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        TupleDomain<ColumnHandle> dynamicPredicate = TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) idColumn, Domain.singleValue(BIGINT, 11L)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        TupleDomain<PaimonColumnHandle> effectivePredicate = PaimonSplitManager.effectivePredicate(
                tableHandle,
                dynamicFilter(dynamicPredicate, false));

        assertThat(effectivePredicate).isEqualTo(staticPredicate);
    }

    @Test
    public void testNoneDynamicPredicateShortCircuitsSplitManager()
    {
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());

        TupleDomain<PaimonColumnHandle> effectivePredicate = PaimonSplitManager.effectivePredicate(
                tableHandle,
                dynamicFilter(TupleDomain.none(), false));

        assertThat(effectivePredicate).isEqualTo(TupleDomain.none());
    }

    @Test
    public void testDynamicPredicateIsIgnoredWhenLimitAlreadyAccepted()
    {
        PaimonColumnHandle idColumn = PaimonColumnHandle.of("id", DataTypes.BIGINT());
        PaimonColumnHandle regionColumn = PaimonColumnHandle.of("region", DataTypes.BIGINT());
        TupleDomain<PaimonColumnHandle> staticPredicate = TupleDomain.withColumnDomains(Map.of(
                regionColumn, Domain.singleValue(BIGINT, 7L)));
        TupleDomain<ColumnHandle> dynamicPredicate = TupleDomain.withColumnDomains(Map.of(
                (ColumnHandle) idColumn, Domain.singleValue(BIGINT, 11L)));
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                staticPredicate,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));

        TupleDomain<PaimonColumnHandle> effectivePredicate = PaimonSplitManager.effectivePredicate(
                tableHandle,
                dynamicFilter(dynamicPredicate, false));

        assertThat(effectivePredicate).isEqualTo(staticPredicate);
    }

    @Test
    public void testConstructorRejectsNullDependencies()
    {
        PaimonTableHandle tableHandle = new PaimonTableHandle(
                "schema",
                "table",
                Collections.emptyMap(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        PaimonCatalog catalog = new PaimonCatalog(fromMap(Map.of()), _ -> {
            throw new UnsupportedOperationException("not used");
        });
        DynamicFilter dynamicFilter = dynamicFilter(TupleDomain.all(), false);
        Duration waitTimeout = new Duration(0, MILLISECONDS);

        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(null, TestingConnectorSession.SESSION, catalog, dynamicFilter, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableHandle is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, null, catalog, dynamicFilter, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("session is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, TestingConnectorSession.SESSION, null, dynamicFilter, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonCatalog is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, TestingConnectorSession.SESSION, catalog, null, waitTimeout))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicFilter is null");
        assertThatThrownBy(() -> new DynamicFilteringTrinoSplitSource(tableHandle, TestingConnectorSession.SESSION, catalog, dynamicFilter, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("dynamicFilteringWaitTimeout is null");
    }

    @Test
    public void testGetNextBatchRejectsNonPositiveBatchSizeBeforeWaitingForDynamicFilter()
    {
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.SESSION,
                new PaimonCatalog(fromMap(Map.of()), _ -> {
                    throw new UnsupportedOperationException("not used");
                }),
                dynamicFilter(TupleDomain.all(), true),
                new Duration(1, SECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(0, DynamicFilterSnapshot.EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot fetch a batch of zero size");
        assertThatThrownBy(() -> splitSource.getNextBatch(-1, DynamicFilterSnapshot.EMPTY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot fetch a batch of zero size");
    }

    @Test
    public void testPlanningInitializesCatalogBeforeLoadingTable()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(false);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get();

        assertThat(catalog.initialized()).isTrue();
        assertThat(catalog.tableLoaded()).isTrue();
        assertThat(batch).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testDynamicSplitPlanningRefreshesLatestFileStoreSchema()
            throws Exception
    {
        AtomicBoolean copiedWithLatestSchema = new AtomicBoolean();
        RecordingCatalog catalog = new RecordingCatalog(false, staleFileStoreTable(copiedWithLatestSchema));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get();

        assertThat(copiedWithLatestSchema).isTrue();
        assertThat(batch).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testDynamicSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Paimon table read uses features which are not supported by the Trino connector: unsupported scan mode");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testDynamicTableChangesSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Paimon system.table_changes uses features which are not supported by the Trino connector: unsupported scan mode");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testDynamicAutoTagTableChangesSplitPlanningMapsUnsupportedReadFeaturesToNotSupported()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, unsupportedPlanningTable());
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Paimon system.table_changes uses features which are not supported by the Trino connector: unsupported scan mode");
                    assertThat(exception.getCause()).isInstanceOf(UnsupportedOperationException.class)
                            .hasMessage("unsupported scan mode");
                });
    }

    @Test
    public void testDynamicSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("dynamic split planning failed"));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to plan Paimon splits: dynamic split planning failed");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("dynamic split planning failed");
                });
    }

    @Test
    public void testDynamicSplitPlanningMapsUnexpectedRuntimeFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingRuntimePlanningTable());
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage("Failed to plan Paimon splits: Index 1 out of bounds for length 1");
                    assertThat(exception.getCause()).isInstanceOf(IndexOutOfBoundsException.class)
                            .hasMessage("Index 1 out of bounds for length 1");
                });
    }

    @Test
    public void testDynamicTableChangesSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("dynamic table_changes planning failed"));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(CoreOptions.INCREMENTAL_BETWEEN.key(), "1,2"),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Failed to plan Paimon table_changes splits: dynamic table_changes planning failed");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("dynamic table_changes planning failed");
                });
    }

    @Test
    public void testDynamicAutoTagTableChangesSplitPlanningMapsWrappedRuntimeIoFailuresToCannotOpenSplit()
    {
        RecordingCatalog catalog = new RecordingCatalog(false, failingPlanningTable("dynamic auto-tag table_changes planning failed"));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Map.of(CoreOptions.INCREMENTAL_TO_AUTO_TAG.key(), "2024-12-04"),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        assertThatThrownBy(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY))
                .isInstanceOfSatisfying(TrinoException.class, exception -> {
                    assertThat(exception.getErrorCode()).isEqualTo(PAIMON_CANNOT_OPEN_SPLIT.toErrorCode());
                    assertThat(exception).hasMessage(
                            "Failed to plan Paimon table_changes splits: dynamic auto-tag table_changes planning failed");
                    assertThat(exception.getCause()).isInstanceOf(IOException.class)
                            .hasMessage("dynamic auto-tag table_changes planning failed");
                });
    }

    @Test
    public void testEmptyPlanningDoesNotInitializeCatalog()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(true);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.none(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get();

        assertThat(catalog.initialized()).isFalse();
        assertThat(catalog.tableLoaded()).isFalse();
        assertThat(batch).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testCloseBeforePlanningDoesNotInitializeCatalog()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(true);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        splitSource.close();

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get();

        assertThat(splitSource.isFinished()).isTrue();
        assertThat(catalog.initialized()).isFalse();
        assertThat(catalog.tableLoaded()).isFalse();
        assertThat(batch).isEmpty();
    }

    @Test
    public void testAcceptedLimitShortCircuitsCurrentNoneDynamicFilter()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(true);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.of(5)),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                blockingDynamicFilter(TupleDomain.none()),
                new Duration(1, SECONDS));

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get();

        assertThat(catalog.initialized()).isFalse();
        assertThat(catalog.tableLoaded()).isFalse();
        assertThat(batch).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testAcceptedLimitSkipsAwaitingNonNoneDynamicFilter()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(false);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.of(5)),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                blockingDynamicFilter(TupleDomain.all()),
                new Duration(1, SECONDS));

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get();

        assertThat(catalog.initialized()).isTrue();
        assertThat(catalog.tableLoaded()).isTrue();
        assertThat(batch).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testUnblockedAwaitableDynamicFilterPlansSplitsImmediately()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(false);
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), true),
                new Duration(1, SECONDS));

        List<ConnectorSplit> batch = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY).get();

        assertThat(catalog.initialized()).isTrue();
        assertThat(catalog.tableLoaded()).isTrue();
        assertThat(batch).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testCloseWhileAwaitingDynamicFilterReturnsFinishedBatch()
            throws Exception
    {
        RecordingCatalog catalog = new RecordingCatalog(true);
        AtomicReference<CompletableFuture<?>> blocked = new AtomicReference<>(new CompletableFuture<>());
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                blockingDynamicFilter(TupleDomain.none(), blocked),
                new Duration(1, SECONDS));

        CompletableFuture<List<ConnectorSplit>> batchFuture = splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY);
        assertThat(batchFuture).isNotDone();

        splitSource.close();
        blocked.get().complete(null);

        List<ConnectorSplit> batch = batchFuture.get();

        assertThat(splitSource.isFinished()).isTrue();
        assertThat(catalog.initialized()).isFalse();
        assertThat(catalog.tableLoaded()).isFalse();
        assertThat(batch).isEmpty();
    }

    @Test
    public void testCloseDoesNotBlockWhileSplitPlanningIsRunning()
            throws Exception
    {
        CountDownLatch planningStarted = new CountDownLatch(1);
        CountDownLatch releasePlanning = new CountDownLatch(1);
        RecordingCatalog catalog = new RecordingCatalog(false, blockingPlanningTable(planningStarted, releasePlanning));
        DynamicFilteringTrinoSplitSource splitSource = new DynamicFilteringTrinoSplitSource(
                new PaimonTableHandle(
                        "schema",
                        "table",
                        Collections.emptyMap(),
                        TupleDomain.all(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty()),
                TestingConnectorSession.builder()
                        .setPropertyMetadata(new PaimonSessionProperties().getSessionProperties())
                        .build(),
                catalog,
                dynamicFilter(TupleDomain.all(), false),
                new Duration(0, MILLISECONDS));

        ExecutorService planningExecutor = Executors.newSingleThreadExecutor();
        ExecutorService closeExecutor = Executors.newSingleThreadExecutor();
        try {
            Future<CompletableFuture<List<ConnectorSplit>>> planningFuture =
                    planningExecutor.submit(() -> splitSource.getNextBatch(100, DynamicFilterSnapshot.EMPTY));
            assertThat(planningStarted.await(5, SECONDS)).isTrue();

            Future<?> closeFuture = closeExecutor.submit(splitSource::close);
            closeFuture.get(1, SECONDS);
            assertThat(splitSource.isFinished()).isTrue();

            releasePlanning.countDown();
            List<ConnectorSplit> batch = planningFuture.get(5, SECONDS).get(5, SECONDS);

            assertThat(batch).isEmpty();
            assertThat(splitSource.isFinished()).isTrue();
        }
        finally {
            releasePlanning.countDown();
            planningExecutor.shutdownNow();
            closeExecutor.shutdownNow();
        }
    }

    private static DynamicFilter dynamicFilter(TupleDomain<ColumnHandle> predicate, boolean awaitable)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return predicate.getDomains()
                        .map(Map::keySet)
                        .orElse(Set.of());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return NOT_BLOCKED;
            }

            @Override
            public boolean isComplete()
            {
                return !awaitable;
            }

            @Override
            public boolean isAwaitable()
            {
                return awaitable;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate;
            }
        };
    }

    private static DynamicFilter blockingDynamicFilter(TupleDomain<ColumnHandle> predicate)
    {
        return blockingDynamicFilter(predicate, new AtomicReference<>(new CompletableFuture<>()));
    }

    private static DynamicFilter blockingDynamicFilter(
            TupleDomain<ColumnHandle> predicate,
            AtomicReference<CompletableFuture<?>> blockedFuture)
    {
        requireNonNull(blockedFuture, "blockedFuture is null");
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return predicate.getDomains()
                        .map(Map::keySet)
                        .orElse(Set.of());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return blockedFuture.get();
            }

            @Override
            public boolean isComplete()
            {
                return false;
            }

            @Override
            public boolean isAwaitable()
            {
                return true;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate;
            }
        };
    }

    private static Table table()
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> readBuilder(rowType);
                    case "rowType" -> rowType;
                    case "toString" -> "testing-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static FileStoreTable staleFileStoreTable(AtomicBoolean copiedWithLatestSchema)
    {
        RowType latestRowType = DataTypes.ROW(DataTypes.FIELD(0, "new_id", DataTypes.BIGINT()));
        FileStoreTable latestTable = (FileStoreTable) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> proxy;
                    case "newReadBuilder" -> readBuilder(latestRowType);
                    case "coreOptions" -> new CoreOptions(new Options());
                    case "rowType" -> latestRowType;
                    case "toString" -> "latest-dynamic-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (FileStoreTable) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {FileStoreTable.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "copyWithLatestSchema" -> {
                        copiedWithLatestSchema.set(true);
                        yield latestTable;
                    }
                    case "coreOptions" -> new CoreOptions(new Options());
                    case "newReadBuilder" -> throw new AssertionError("stale table must not be used for dynamic split planning");
                    case "rowType" -> throw new AssertionError("stale rowType must not be used for dynamic split planning");
                    case "toString" -> "stale-dynamic-file-store-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder readBuilder(RowType rowType)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "dropStats" -> throw new AssertionError("dropStats must not be called before scan planning");
                    case "withFilter", "withLimit" -> proxy;
                    case "newScan" -> tableScan();
                    case "readType" -> rowType;
                    case "tableName" -> "testing-table";
                    case "toString" -> "testing-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table unsupportedPlanningTable()
    {
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> unsupportedPlanningReadBuilder();
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "toString" -> "unsupported-dynamic-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table failingPlanningTable(String message)
    {
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> failingPlanningReadBuilder(message);
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "toString" -> "failing-dynamic-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table failingRuntimePlanningTable()
    {
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> failingRuntimePlanningReadBuilder();
                    case "rowType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "toString" -> "runtime-failing-dynamic-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder unsupportedPlanningReadBuilder()
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> throw new UnsupportedOperationException("unsupported scan mode");
                    case "readType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "tableName" -> "unsupported-dynamic-planning-table";
                    case "toString" -> "unsupported-dynamic-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder failingPlanningReadBuilder(String message)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> throw new UncheckedIOException(new IOException(message));
                    case "readType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "tableName" -> "failing-dynamic-planning-table";
                    case "toString" -> "failing-dynamic-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder failingRuntimePlanningReadBuilder()
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> throw new IndexOutOfBoundsException("Index 1 out of bounds for length 1");
                    case "readType" -> DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
                    case "tableName" -> "runtime-failing-dynamic-planning-table";
                    case "toString" -> "runtime-failing-dynamic-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Table blockingPlanningTable(CountDownLatch planningStarted, CountDownLatch releasePlanning)
    {
        RowType rowType = DataTypes.ROW(DataTypes.FIELD(0, "id", DataTypes.BIGINT()));
        return (Table) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "copy" -> proxy;
                    case "newReadBuilder" -> blockingPlanningReadBuilder(rowType, planningStarted, releasePlanning);
                    case "rowType" -> rowType;
                    case "toString" -> "blocking-dynamic-planning-table";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ReadBuilder blockingPlanningReadBuilder(
            RowType rowType,
            CountDownLatch planningStarted,
            CountDownLatch releasePlanning)
    {
        return (ReadBuilder) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {ReadBuilder.class},
                (proxy, method, _) -> switch (method.getName()) {
                    case "dropStats", "withFilter", "withLimit" -> proxy;
                    case "newScan" -> {
                        planningStarted.countDown();
                        try {
                            assertThat(releasePlanning.await(5, SECONDS)).isTrue();
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        yield tableScan();
                    }
                    case "readType" -> rowType;
                    case "tableName" -> "blocking-dynamic-planning-table";
                    case "toString" -> "blocking-dynamic-planning-read-builder";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static TableScan tableScan()
    {
        return (TableScan) Proxy.newProxyInstance(
                DynamicFilteringTrinoSplitSourceTest.class.getClassLoader(),
                new Class<?>[] {TableScan.class},
                (_, method, _) -> switch (method.getName()) {
                    case "plan" -> (TableScan.Plan) () -> List.of();
                    case "toString" -> "testing-table-scan";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static class RecordingCatalog
            extends PaimonCatalog
    {
        private final boolean failIfInitialized;
        private final Table table;
        private final AtomicBoolean initialized = new AtomicBoolean();
        private final AtomicBoolean tableLoaded = new AtomicBoolean();

        private RecordingCatalog(boolean failIfInitialized)
        {
            this(failIfInitialized, table());
        }

        private RecordingCatalog(boolean failIfInitialized, Table table)
        {
            super(new Options(), _ -> {
                throw new AssertionError("filesystem should not be used by dynamic filtering split source test");
            });
            this.failIfInitialized = failIfInitialized;
            this.table = table;
        }

        @Override
        public void initSession(ConnectorSession connectorSession)
        {
            if (failIfInitialized) {
                throw new AssertionError("catalog should not be initialized for empty split planning");
            }
            initialized.set(true);
        }

        @Override
        public Catalog forSession(ConnectorSession connectorSession)
        {
            if (failIfInitialized) {
                throw new AssertionError("catalog should not be initialized for empty split planning");
            }
            initialized.set(true);
            return this;
        }

        @Override
        public Table getTable(Identifier identifier)
        {
            if (!initialized.get()) {
                throw new AssertionError("table loaded before catalog session initialization");
            }
            tableLoaded.set(true);
            assertThat(identifier.getDatabaseName()).isEqualTo("schema");
            assertThat(identifier.getObjectName()).isEqualTo("table");
            return table;
        }

        private boolean initialized()
        {
            return initialized.get();
        }

        private boolean tableLoaded()
        {
            return tableLoaded.get();
        }
    }
}
