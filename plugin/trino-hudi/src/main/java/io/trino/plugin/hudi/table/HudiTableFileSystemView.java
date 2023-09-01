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
package io.trino.plugin.hudi.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.plugin.hudi.compaction.CompactionOperation;
import io.trino.plugin.hudi.compaction.HudiCompactionOperation;
import io.trino.plugin.hudi.compaction.HudiCompactionPlan;
import io.trino.plugin.hudi.files.HudiBaseFile;
import io.trino.plugin.hudi.files.HudiFileGroup;
import io.trino.plugin.hudi.files.HudiFileGroupId;
import io.trino.plugin.hudi.files.HudiLogFile;
import io.trino.plugin.hudi.model.HudiFileFormat;
import io.trino.plugin.hudi.model.HudiInstant;
import io.trino.plugin.hudi.model.HudiReplaceCommitMetadata;
import io.trino.plugin.hudi.timeline.HudiTimeline;
import io.trino.spi.TrinoException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_BAD_DATA;
import static io.trino.plugin.hudi.files.FSUtils.LOG_FILE_PATTERN;
import static io.trino.plugin.hudi.files.FSUtils.getPartitionLocation;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;

public class HudiTableFileSystemView
{
    private static final Logger LOG = Logger.get(HudiTableFileSystemView.class);
    private static final Integer VERSION_2 = 2;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    // Locks to control concurrency. Sync operations use write-lock blocking all fetch operations.
    // For the common-case, we allow concurrent read of single or multiple partitions
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = globalLock.readLock();
    // Used to concurrently load and populate partition views
    private final ConcurrentHashMap<String, Boolean> addedPartitions = new ConcurrentHashMap<>(4096);

    private boolean closed;

    private Map<String, List<HudiFileGroup>> partitionToFileGroupsMap;
    private HudiTableMetaClient metaClient;

    private Map<HudiFileGroupId, Entry<String, CompactionOperation>> fgIdToPendingCompaction;

    private HudiTimeline visibleCommitsAndCompactionTimeline;

    private Map<HudiFileGroupId, HudiInstant> fgIdToReplaceInstants;

    public HudiTableFileSystemView(HudiTableMetaClient metaClient, HudiTimeline visibleActiveTimeline)
    {
        partitionToFileGroupsMap = new ConcurrentHashMap<>();
        this.metaClient = metaClient;
        this.visibleCommitsAndCompactionTimeline = visibleActiveTimeline.getWriteTimeline();
        resetFileGroupsReplaced(visibleCommitsAndCompactionTimeline);
        resetPendingCompactionOperations(getAllPendingCompactionOperations(metaClient)
                .values().stream()
                .map(pair -> Map.entry(pair.getKey(), CompactionOperation.convertFromAvroRecordInstance(pair.getValue()))));
    }

    private static Map<HudiFileGroupId, Entry<String, HudiCompactionOperation>> getAllPendingCompactionOperations(
            HudiTableMetaClient metaClient)
    {
        List<Entry<HudiInstant, HudiCompactionPlan>> pendingCompactionPlanWithInstants =
                getAllPendingCompactionPlans(metaClient);

        Map<HudiFileGroupId, Entry<String, HudiCompactionOperation>> fgIdToPendingCompactionWithInstantMap = new HashMap<>();
        pendingCompactionPlanWithInstants.stream()
                .flatMap(instantPlanPair -> getPendingCompactionOperations(instantPlanPair.getKey(), instantPlanPair.getValue()))
                .forEach(pair -> {
                    if (fgIdToPendingCompactionWithInstantMap.containsKey(pair.getKey())) {
                        HudiCompactionOperation operation = pair.getValue().getValue();
                        HudiCompactionOperation anotherOperation = fgIdToPendingCompactionWithInstantMap.get(pair.getKey()).getValue();

                        if (!operation.equals(anotherOperation)) {
                            String msg = "Hudi File Id (" + pair.getKey() + ") has more than 1 pending compactions. Instants: "
                                    + pair.getValue() + ", " + fgIdToPendingCompactionWithInstantMap.get(pair.getKey());
                            throw new IllegalStateException(msg);
                        }
                    }
                    fgIdToPendingCompactionWithInstantMap.put(pair.getKey(), pair.getValue());
                });
        return fgIdToPendingCompactionWithInstantMap;
    }

    private static List<Entry<HudiInstant, HudiCompactionPlan>> getAllPendingCompactionPlans(
            HudiTableMetaClient metaClient)
    {
        List<HudiInstant> pendingCompactionInstants =
                metaClient.getActiveTimeline()
                        .filterPendingCompactionTimeline()
                        .getInstants()
                        .collect(toImmutableList());
        return pendingCompactionInstants.stream()
                .map(instant -> {
                    try {
                        return Map.entry(instant, getCompactionPlan(metaClient, instant.getTimestamp()));
                    }
                    catch (IOException e) {
                        throw new TrinoException(HUDI_BAD_DATA, e);
                    }
                })
                .collect(toImmutableList());
    }

    private static HudiCompactionPlan getCompactionPlan(HudiTableMetaClient metaClient, String compactionInstant)
            throws IOException
    {
        HudiCompactionPlan compactionPlan = deserializeAvroMetadata(
                metaClient
                        .getActiveTimeline()
                        .readCompactionPlanAsBytes(HudiTimeline.getCompactionRequestedInstant(compactionInstant)).get(),
                HudiCompactionPlan.class);
        return upgradeToLatest(compactionPlan, compactionPlan.getVersion());
    }

    private static HudiCompactionPlan upgradeToLatest(HudiCompactionPlan metadata, int metadataVersion)
    {
        if (metadataVersion == VERSION_2) {
            return metadata;
        }
        checkState(metadataVersion == 1, "Lowest supported metadata version is 1");
        List<HudiCompactionOperation> v2CompactionOperationList = new ArrayList<>();
        if (null != metadata.getOperations()) {
            v2CompactionOperationList = metadata.getOperations().stream()
                    .map(compactionOperation ->
                            HudiCompactionOperation.newBuilder()
                                    .setBaseInstantTime(compactionOperation.getBaseInstantTime())
                                    .setFileId(compactionOperation.getFileId())
                                    .setPartitionPath(compactionOperation.getPartitionPath())
                                    .setMetrics(compactionOperation.getMetrics())
                                    .setDataFilePath(compactionOperation.getDataFilePath() == null ? null : Location.of(compactionOperation.getDataFilePath()).fileName())
                                    .setDeltaFilePaths(compactionOperation.getDeltaFilePaths().stream().map(filePath -> Location.of(filePath).fileName()).collect(toImmutableList()))
                                    .build())
                    .collect(toImmutableList());
        }
        return new HudiCompactionPlan(v2CompactionOperationList, metadata.getExtraMetadata(), VERSION_2);
    }

    private static <T extends SpecificRecordBase> T deserializeAvroMetadata(byte[] bytes, Class<T> clazz)
            throws IOException
    {
        DatumReader<T> reader = new SpecificDatumReader<>(clazz);
        FileReader<T> fileReader = DataFileReader.openReader(new SeekableByteArrayInput(bytes), reader);
        checkState(fileReader.hasNext(), "Could not deserialize metadata of type " + clazz);
        return fileReader.next();
    }

    private static Stream<Entry<HudiFileGroupId, Entry<String, HudiCompactionOperation>>> getPendingCompactionOperations(
            HudiInstant instant, HudiCompactionPlan compactionPlan)
    {
        List<HudiCompactionOperation> ops = compactionPlan.getOperations();
        if (null != ops) {
            return ops.stream().map(op -> Map.entry(
                    new HudiFileGroupId(op.getPartitionPath(), op.getFileId()),
                    Map.entry(instant.getTimestamp(), op)));
        }
        return Stream.empty();
    }

    private void resetPendingCompactionOperations(Stream<Entry<String, CompactionOperation>> operations)
    {
        this.fgIdToPendingCompaction = operations.collect(toImmutableMap(
                entry -> entry.getValue().getFileGroupId(),
                identity()));
    }

    private void resetFileGroupsReplaced(HudiTimeline timeline)
    {
        // for each REPLACE instant, get map of (partitionPath -> deleteFileGroup)
        HudiTimeline replacedTimeline = timeline.getCompletedReplaceTimeline();
        Map<HudiFileGroupId, HudiInstant> replacedFileGroups = replacedTimeline.getInstants()
                .flatMap(instant -> {
                    try {
                        HudiReplaceCommitMetadata replaceMetadata = HudiReplaceCommitMetadata.fromBytes(
                                metaClient.getActiveTimeline().getInstantDetails(instant).get(),
                                OBJECT_MAPPER,
                                HudiReplaceCommitMetadata.class);

                        // get replace instant mapping for each partition, fileId
                        return replaceMetadata.getPartitionToReplaceFileIds().entrySet().stream()
                                .flatMap(entry -> entry.getValue().stream().map(fileId ->
                                        Map.entry(new HudiFileGroupId(entry.getKey(), fileId), instant)));
                    }
                    catch (IOException e) {
                        throw new TrinoException(HUDI_BAD_DATA, "error reading commit metadata for " + instant, e);
                    }
                })
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));
        fgIdToReplaceInstants = new ConcurrentHashMap<>(replacedFileGroups);
    }

    public final Stream<HudiBaseFile> getLatestBaseFiles(String partitionStr)
    {
        try {
            readLock.lock();
            String partitionPath = formatPartitionKey(partitionStr);
            ensurePartitionLoadedCorrectly(partitionPath);
            return fetchLatestBaseFiles(partitionPath)
                    .filter(hudiBaseFile -> !isFileGroupReplaced(partitionPath, hudiBaseFile.getFileId()));
        }
        finally {
            readLock.unlock();
        }
    }

    private boolean isFileGroupReplaced(String partitionPath, String fileId)
    {
        return isFileGroupReplaced(new HudiFileGroupId(partitionPath, fileId));
    }

    private String formatPartitionKey(String partitionStr)
    {
        return partitionStr.endsWith("/") ? partitionStr.substring(0, partitionStr.length() - 1) : partitionStr;
    }

    private void ensurePartitionLoadedCorrectly(String partition)
    {
        checkState(!isClosed(), "View is already closed");

        addedPartitions.computeIfAbsent(partition, (partitionPathStr) -> {
            long beginTs = System.currentTimeMillis();
            if (!isPartitionAvailableInStore(partitionPathStr)) {
                // Not loaded yet
                try {
                    LOG.info("Building file system view for partition (" + partitionPathStr + ")");

                    Location partitionLocation = getPartitionLocation(metaClient.getBasePath(), partitionPathStr);
                    FileIterator partitionFiles = listPartition(partitionLocation);
                    List<HudiFileGroup> groups = addFilesToView(partitionFiles);

                    if (groups.isEmpty()) {
                        storePartitionView(partitionPathStr, new ArrayList<>());
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(HUDI_BAD_DATA, "Failed to list base files in partition " + partitionPathStr, e);
                }
            }
            else {
                LOG.debug("View already built for Partition :" + partitionPathStr + ", FOUND is ");
            }
            long endTs = System.currentTimeMillis();
            LOG.debug("Time to load partition (" + partitionPathStr + ") =" + (endTs - beginTs));
            return true;
        });
    }

    protected boolean isPartitionAvailableInStore(String partitionPath)
    {
        return partitionToFileGroupsMap.containsKey(partitionPath);
    }

    private FileIterator listPartition(Location partitionLocation)
            throws IOException
    {
        FileIterator fileIterator = metaClient.getFileSystem().listFiles(partitionLocation);
        if (fileIterator.hasNext()) {
            return fileIterator;
        }
        try (OutputStream ignored = metaClient.getFileSystem().newOutputFile(partitionLocation).create()) {
            return FileIterator.empty();
        }
    }

    public List<HudiFileGroup> addFilesToView(FileIterator partitionFiles)
            throws IOException
    {
        List<HudiFileGroup> fileGroups = buildFileGroups(partitionFiles, visibleCommitsAndCompactionTimeline, true);
        // Group by partition for efficient updates for both InMemory and DiskBased structures.
        fileGroups.stream()
                .collect(groupingBy(HudiFileGroup::getPartitionPath))
                .forEach((partition, value) -> {
                    if (!isPartitionAvailableInStore(partition)) {
                        storePartitionView(partition, value);
                    }
                });
        return fileGroups;
    }

    private List<HudiFileGroup> buildFileGroups(
            FileIterator partitionFiles,
            HudiTimeline timeline,
            boolean addPendingCompactionFileSlice)
            throws IOException
    {
        List<HudiBaseFile> hoodieBaseFiles = new ArrayList<>();
        List<HudiLogFile> hudiLogFiles = new ArrayList<>();
        String baseHoodieFileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
        while (partitionFiles.hasNext()) {
            FileEntry fileEntry = partitionFiles.next();
            if (fileEntry.location().path().contains(baseHoodieFileExtension)) {
                hoodieBaseFiles.add(new HudiBaseFile(fileEntry));
            }
            String fileName = fileEntry.location().fileName();
            if (LOG_FILE_PATTERN.matcher(fileName).matches() && fileName.contains(HudiFileFormat.HOODIE_LOG.getFileExtension())) {
                hudiLogFiles.add(new HudiLogFile(fileEntry));
            }
        }
        return buildFileGroups(hoodieBaseFiles.stream(), hudiLogFiles.stream(), timeline, addPendingCompactionFileSlice);
    }

    private List<HudiFileGroup> buildFileGroups(
            Stream<HudiBaseFile> baseFileStream,
            Stream<HudiLogFile> logFileStream,
            HudiTimeline timeline,
            boolean addPendingCompactionFileSlice)
    {
        Map<Entry<String, String>, List<HudiBaseFile>> baseFiles = baseFileStream
                .collect(groupingBy(baseFile -> {
                    String partitionPathStr = getPartitionPathFor(baseFile);
                    return Map.entry(partitionPathStr, baseFile.getFileId());
                }));

        Map<Entry<String, String>, List<HudiLogFile>> logFiles = logFileStream
                .collect(groupingBy((logFile) -> {
                    String partitionPathStr = getRelativePartitionPath(metaClient.getBasePath(), logFile.getPath().parentDirectory());
                    return Map.entry(partitionPathStr, logFile.getFileId());
                }));

        Set<Entry<String, String>> fileIdSet = new HashSet<>(baseFiles.keySet());
        fileIdSet.addAll(logFiles.keySet());

        List<HudiFileGroup> fileGroups = new ArrayList<>();
        fileIdSet.forEach(pair -> {
            String fileId = pair.getValue();
            String partitionPath = pair.getKey();
            HudiFileGroup group = new HudiFileGroup(partitionPath, fileId, timeline);
            if (baseFiles.containsKey(pair)) {
                baseFiles.get(pair).forEach(group::addBaseFile);
            }
            if (logFiles.containsKey(pair)) {
                logFiles.get(pair).forEach(group::addLogFile);
            }

            if (addPendingCompactionFileSlice) {
                Optional<Entry<String, CompactionOperation>> pendingCompaction =
                        getPendingCompactionOperationWithInstant(group.getFileGroupId());
                // If there is no delta-commit after compaction request, this step would ensure a new file-slice appears
                // so that any new ingestion uses the correct base-instant
                pendingCompaction.ifPresent(entry ->
                        group.addNewFileSliceAtInstant(entry.getKey()));
            }
            fileGroups.add(group);
        });

        return fileGroups;
    }

    private String getPartitionPathFor(HudiBaseFile baseFile)
    {
        return getRelativePartitionPath(metaClient.getBasePath(), baseFile.getFullPath().parentDirectory());
    }

    private String getRelativePartitionPath(Location basePath, Location fullPartitionPath)
    {
        String fullPartitionPathStr = fullPartitionPath.path();

        if (!fullPartitionPathStr.startsWith(basePath.path())) {
            throw new IllegalArgumentException("Partition location does not belong to base-location");
        }

        int partitionStartIndex = fullPartitionPath.path().indexOf(basePath.fileName(), basePath.parentDirectory().path().length());
        // Partition-Path could be empty for non-partitioned tables
        if (partitionStartIndex + basePath.fileName().length() == fullPartitionPathStr.length()) {
            return "";
        }
        return fullPartitionPathStr.substring(partitionStartIndex + basePath.fileName().length() + 1);
    }

    protected Optional<Entry<String, CompactionOperation>> getPendingCompactionOperationWithInstant(HudiFileGroupId fgId)
    {
        return Optional.ofNullable(fgIdToPendingCompaction.get(fgId));
    }

    private void storePartitionView(String partitionPath, List<HudiFileGroup> fileGroups)
    {
        LOG.debug("Adding file-groups for partition :" + partitionPath + ", #FileGroups=" + fileGroups.size());
        List<HudiFileGroup> newList = ImmutableList.copyOf(fileGroups);
        partitionToFileGroupsMap.put(partitionPath, newList);
    }

    private Stream<HudiBaseFile> fetchLatestBaseFiles(final String partitionPath)
    {
        return fetchAllStoredFileGroups(partitionPath)
                .filter(filGroup -> !isFileGroupReplaced(filGroup.getFileGroupId()))
                .map(filGroup -> Map.entry(filGroup.getFileGroupId(), getLatestBaseFile(filGroup)))
                .filter(pair -> pair.getValue().isPresent())
                .map(pair -> pair.getValue().get());
    }

    private Stream<HudiFileGroup> fetchAllStoredFileGroups(String partition)
    {
        final List<HudiFileGroup> fileGroups = ImmutableList.copyOf(partitionToFileGroupsMap.get(partition));
        return fileGroups.stream();
    }

    private boolean isFileGroupReplaced(HudiFileGroupId fileGroup)
    {
        return Optional.ofNullable(fgIdToReplaceInstants.get(fileGroup)).isPresent();
    }

    protected Optional<HudiBaseFile> getLatestBaseFile(HudiFileGroup fileGroup)
    {
        return fileGroup.getAllBaseFiles()
                .filter(hudiBaseFile -> !isBaseFileDueToPendingCompaction(hudiBaseFile) && !isBaseFileDueToPendingClustering(hudiBaseFile))
                .findFirst();
    }

    private boolean isBaseFileDueToPendingCompaction(HudiBaseFile baseFile)
    {
        final String partitionPath = getPartitionPathFor(baseFile);

        Optional<Entry<String, CompactionOperation>> compactionWithInstantTime =
                getPendingCompactionOperationWithInstant(new HudiFileGroupId(partitionPath, baseFile.getFileId()));
        return (compactionWithInstantTime.isPresent()) && (null != compactionWithInstantTime.get().getKey())
                && baseFile.getCommitTime().equals(compactionWithInstantTime.get().getKey());
    }

    private boolean isBaseFileDueToPendingClustering(HudiBaseFile baseFile)
    {
        List<String> pendingReplaceInstants = metaClient.getActiveTimeline()
                .filterPendingReplaceTimeline()
                .getInstants()
                .map(HudiInstant::getTimestamp)
                .collect(toImmutableList());

        return !pendingReplaceInstants.isEmpty() && pendingReplaceInstants.contains(baseFile.getCommitTime());
    }

    public boolean isClosed()
    {
        return closed;
    }

    public void close()
    {
        this.fgIdToPendingCompaction = null;
        this.partitionToFileGroupsMap = null;
        this.fgIdToReplaceInstants = null;
        closed = true;
    }
}
