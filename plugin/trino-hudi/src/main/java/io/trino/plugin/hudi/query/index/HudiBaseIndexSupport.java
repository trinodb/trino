package io.trino.plugin.hudi.query.index;

import io.airlift.log.Logger;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class HudiBaseIndexSupport
        implements HudiIndexSupport
{

    private final Logger log;
    protected final HoodieTableMetaClient metaClient;

    public HudiBaseIndexSupport(Logger log, HoodieTableMetaClient metaClient)
    {
        this.log = requireNonNull(log, "log is null");
        this.metaClient = requireNonNull(metaClient, "metaClient is null");
    }

    public void printDebugMessage(Map<String, List<FileSlice>> candidateFileSlices, Map<String, List<FileSlice>> inputFileSlices)
    {
        if (log.isDebugEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = inputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles * 1.0d);
            log.info("Total files: %s; files after data skipping: %s; skipping percent %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent);
        }
    }
}
