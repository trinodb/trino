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
package io.trino.plugin.varada.dispatcher.warmup.export;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.filesystem.Location;
import io.trino.plugin.varada.VaradaSessionProperties;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.di.VaradaInitializedServiceRegistry;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.WorkerSubmittableTask;
import io.trino.plugin.varada.dispatcher.warmup.WorkerTaskExecutorService;
import io.trino.plugin.varada.dispatcher.warmup.events.WarmingFinishedEvent;
import io.trino.plugin.varada.metrics.MetricsManager;
import io.trino.plugin.varada.util.VaradaInitializedServiceMarker;
import io.trino.plugin.warp.gen.stats.VaradaStatsWarmupExportService;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.tools.util.StringUtils;

import static io.trino.plugin.varada.dispatcher.warmup.WarmUtils.isImportExportEnabled;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarmupExportingService
        implements VaradaInitializedServiceMarker
{
    public static final String WARMUP_EXPORTER_STAT_GROUP = "warmup-exporter";

    private final RowGroupDataService rowGroupDataService;
    private final WarmupElementsCloudExporter warmupElementsCloudExporter;
    private final GlobalConfiguration globalConfiguration;
    private final CloudVendorConfiguration cloudVendorConfiguration;
    private final WorkerTaskExecutorService workerTaskExecutorService;
    private final VaradaStatsWarmupExportService statsWarmupExportService;
    private final CloudVendorService cloudVendorService;

    @Inject
    public WarmupExportingService(
            MetricsManager metricsManager,
            EventBus eventBus,
            RowGroupDataService rowGroupDataService,
            WarmupElementsCloudExporter warmupElementsCloudExporter,
            GlobalConfiguration globalConfiguration,
            WorkerTaskExecutorService workerTaskExecutorService,
            @ForWarp CloudVendorConfiguration cloudVendorConfiguration,
            @ForWarp CloudVendorService cloudVendorService,
            VaradaInitializedServiceRegistry varadaInitializedServiceRegistry)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.warmupElementsCloudExporter = warmupElementsCloudExporter;
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.cloudVendorConfiguration = requireNonNull(cloudVendorConfiguration);
        this.cloudVendorService = requireNonNull(cloudVendorService);
        requireNonNull(eventBus).register(this);
        this.statsWarmupExportService = requireNonNull(metricsManager).registerMetric(new VaradaStatsWarmupExportService(WARMUP_EXPORTER_STAT_GROUP));
        if (isImportExportEnabled(globalConfiguration, cloudVendorConfiguration, null)) {
            varadaInitializedServiceRegistry.addService(this);
            validateS3ImportExportPath();
        }
    }

    private void validateS3ImportExportPath()
    {
        if (StringUtils.isEmpty(cloudVendorConfiguration.getStorePath())) {
            throw new RuntimeException("backup location is not available");
        }
        String s3ImportExportPath = VaradaSessionProperties.getS3ImportExportPath(null, cloudVendorConfiguration, cloudVendorService);
        Location location = cloudVendorService.getLocation(s3ImportExportPath);
        String pathToValidate = s3ImportExportPath.substring(0, s3ImportExportPath.indexOf(location.path()));
        if (!cloudVendorService.directoryExists(pathToValidate)) {
            throw new RuntimeException("backup location is not available");
        }
    }

    @Override
    public void init()
    {
    }

    void export(String cloudImportExportPath, RowGroupKey rowGroupKey, int delay)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        if (rowGroupData == null) {
            statsWarmupExportService.incexport_skipped_due_row_group();
            return;
        }

        WeGroupCloudExporterTask weGroupCloudExporterTask = new WeGroupCloudExporterTask(rowGroupKey,
                cloudImportExportPath,
                workerTaskExecutorService,
                rowGroupDataService,
                warmupElementsCloudExporter,
                globalConfiguration,
                statsWarmupExportService);
        workerTaskExecutorService.delaySubmit(delay, weGroupCloudExporterTask, this::handleConflict);
    }

    private void handleConflict(WorkerSubmittableTask task)
    {
        statsWarmupExportService.incexport_skipped_due_key_conflict();
    }

    @Subscribe
    public void warmingFinished(WarmingFinishedEvent warmingFinishedEvent)
    {
        if (!isImportExportEnabled(globalConfiguration, cloudVendorConfiguration, warmingFinishedEvent.session())) {
            // mostly for integration test, but maybe it's good to skip in this case in general
            return;
        }
        String s3ImportExportPath = VaradaSessionProperties.getS3ImportExportPath(
                warmingFinishedEvent.session(),
                cloudVendorConfiguration,
                cloudVendorService);

        int exportDelay = globalConfiguration.getExportDelayInSeconds();
        export(s3ImportExportPath, warmingFinishedEvent.rowGroupKey(), exportDelay);
    }
}
