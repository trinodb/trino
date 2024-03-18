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
package io.trino.plugin.varada.storage.write;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.filesystem.Location;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.dispatcher.model.RowGroupData;
import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.services.RowGroupDataService;
import io.trino.plugin.varada.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarpCacheFilesMerger
{
    private static final int BUFFER_SIZE = 8192;
    private final RowGroupDataService rowGroupDataService;
    private final StorageWarmerService storageWarmerService;
    private final GlobalConfiguration globalConfiguration;
    private final int pageSizeShift;

    @Inject
    public WarpCacheFilesMerger(RowGroupDataService rowGroupDataService, StorageWarmerService storageWarmerService, GlobalConfiguration globalConfiguration, StorageEngineConstants storageEngineConstants)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.storageWarmerService = requireNonNull(storageWarmerService);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        this.pageSizeShift = requireNonNull(storageEngineConstants).getPageSizeShift();
    }

    public void mergeTmpFiles(List<RowGroupData> tmpRowGroupDataList, RowGroupKey permanentRowGroupKey)
            throws IOException
    {
        RowGroupData permanentRowGroupData = rowGroupDataService.getOrCreateRowGroupData(permanentRowGroupKey, Collections.emptyMap());
        storageWarmerService.createFile(permanentRowGroupKey);
        boolean allWeAreValid = tmpRowGroupDataList.stream().flatMap(x -> x.getValidWarmUpElements().stream()).allMatch(WarmUpElement::isValid);
        int maxOffset = permanentRowGroupData.getNextOffset();
        if (allWeAreValid) {
            String permanentRowGroupPath = permanentRowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
            try (RandomAccessFile mergedFile = new RandomAccessFile(permanentRowGroupPath, "rw")) {
                int offset = maxOffset << pageSizeShift;
                mergedFile.seek(offset);
                for (RowGroupData tmpRowGroupData : tmpRowGroupDataList) {
                    List<WarmUpElement> validWarmUpElements = tmpRowGroupData.getValidWarmUpElements();
                    WarmUpElement warmUpElement = validWarmUpElements.get(0); //todo: we know we have only 1 column
                    checkArgument(warmUpElement.getStartOffset() == 0, "start offset must be zero but wasn't warmUpElement=%s", warmUpElement);
                    int relativeStartOffset = maxOffset;
                    int relativeEndOffset = maxOffset + (warmUpElement.getEndOffset() - warmUpElement.getStartOffset());
                    int relativeQueryOffset = warmUpElement.getQueryOffset() + maxOffset;
                    WarmUpElement newWarmupElement = WarmUpElement.builder(warmUpElement)
                            .startOffset(relativeStartOffset)
                            .endOffset(relativeEndOffset)
                            .queryOffset(relativeQueryOffset)
                            .build();
                    copyFileContent(tmpRowGroupData.getRowGroupKey(), permanentRowGroupPath, mergedFile, tmpRowGroupData.getNextOffset() << pageSizeShift);
                    maxOffset += tmpRowGroupData.getNextOffset();
                    permanentRowGroupData = rowGroupDataService.updateRowGroupData(permanentRowGroupData,
                            newWarmupElement,
                            maxOffset,
                            newWarmupElement.getTotalRecords());
                    maxOffset = permanentRowGroupData.getNextOffset();
                }
                rowGroupDataService.flush(permanentRowGroupKey);
            }
        }
        else {
            //in case of failure, add all WE as failed to permanent RG
            for (RowGroupData tmpRowGroupData : tmpRowGroupDataList) {
                rowGroupDataService.markAsFailed(permanentRowGroupKey, tmpRowGroupData.getWarmUpElements(), Collections.emptyMap());
            }
        }
    }

    private void copyFileContent(RowGroupKey tmpRowGroupKey, String permanentRowGroupPath, RandomAccessFile mergedFile, int length)
    {
        String tmpRowGroupFilePath = tmpRowGroupKey.stringFileNameRepresentation(globalConfiguration.getLocalStorePath());
        validateAndCreateDirectoryIfNeeded(tmpRowGroupFilePath, permanentRowGroupPath);
        try (RandomAccessFile reader = new RandomAccessFile(tmpRowGroupFilePath, "r")) {
            byte[] buffer = new byte[BUFFER_SIZE];

            // Read from the source file and write to the destination file
            int bytesRead;
            int totalBytesRead = 0;
            while (totalBytesRead < length && (bytesRead = reader.read(buffer, 0, Math.min(BUFFER_SIZE, length - totalBytesRead))) != -1) {
                mergedFile.write(buffer, 0, bytesRead);
                totalBytesRead += bytesRead;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateAndCreateDirectoryIfNeeded(String tmpRowGroupPath, String permanentRowGroupPath)
    {
        validateLocation(tmpRowGroupPath);
        try {
            Path permanentPath = getPath(permanentRowGroupPath);

            Files.createDirectories(permanentPath.getParent());
            if (!permanentPath.toFile().exists()) {
                Files.createFile(permanentPath); // Create if not exists
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateLocation(String path)
    {
        requireNonNull(getLocation(path));
    }

    private Path getPath(String path, String... pathElements)
    {
        String cleanPath = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return Path.of(cleanPath.replaceFirst("file://", ""), pathElements);
    }

    /**
     * List of string where bucket is first and the rest are the path parts
     * param str - S3 path
     */
    public Location getLocation(String path)
    {
        return Location.of(path);
    }

    public void deleteTmpRowGroups(List<RowGroupData> tmpRowGroupDataList)
    {
        for (RowGroupData tmpRowGroupData : tmpRowGroupDataList) {
            rowGroupDataService.deleteFile(tmpRowGroupData);
        }
    }
}
