package org.apache.hadoop.hive.ql.io;

import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.common.util.Ref;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.ql.io.AcidUtils.BASE_PREFIX;
import static org.apache.hadoop.hive.ql.io.AcidUtils.DELETE_DELTA_PREFIX;
import static org.apache.hadoop.hive.ql.io.AcidUtils.DELTA_PREFIX;
import static org.apache.hadoop.hive.ql.io.AcidUtils.createOriginalObj;
import static org.apache.hadoop.hive.ql.io.AcidUtils.hiddenFileFilter;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;

@SuppressWarnings("ALL")
public class OurAcidUtils
{
    private static final Logger LOG = Logger.get(OurAcidUtils.class);
    private static final HadoopShims SHIMS = ShimLoader.getHadoopShims();

    /**
     * Get the ACID state of the given directory. It finds the minimal set of
     * base and diff directories. Note that because major compactions don't
     * preserve the history, we can't use a base directory that includes a
     * write id that we must exclude.
     * @param directory the partition directory to analyze
     * @param conf the configuration
     * @param writeIdList the list of write ids that we are reading
     * @return the state of the directory
     * @throws IOException
     */
    public static Directory getAcidState(Path directory,
            Configuration conf,
            ValidWriteIdList writeIdList,
            boolean useFileIds,
            boolean ignoreEmptyFiles
    ) throws IOException
    {
        return getAcidState(directory, conf, writeIdList, Ref.from(useFileIds), ignoreEmptyFiles, null);
    }

    public static Directory getAcidState(Path directory,
            Configuration conf,
            ValidWriteIdList writeIdList,
            Ref<Boolean> useFileIds,
            boolean ignoreEmptyFiles,
            Map<String, String> tblproperties) throws IOException {
        FileSystem fs = directory.getFileSystem(conf);
        // The following 'deltas' includes all kinds of delta files including insert & delete deltas.
        final List<ParsedDelta> deltas = new ArrayList<ParsedDelta>();
        List<ParsedDelta> working = new ArrayList<ParsedDelta>();
        List<FileStatus> originalDirectories = new ArrayList<FileStatus>();
        final List<FileStatus> obsolete = new ArrayList<FileStatus>();
        final List<FileStatus> abortedDirectories = new ArrayList<>();
        List<HdfsFileStatusWithId> childrenWithId = null;
        Boolean val = useFileIds.value;
        if (val == null || val) {
            try {
                childrenWithId = SHIMS.listLocatedHdfsStatus(fs, directory, hiddenFileFilter);
                if (val == null) {
                    useFileIds.value = true;
                }
            } catch (Throwable t) {
                LOG.error("Failed to get files with ID; using regular API: " + t.getMessage());
                if (val == null && t instanceof UnsupportedOperationException) {
                    useFileIds.value = false;
                }
            }
        }
        TxnBase bestBase = new TxnBase();
        final List<HdfsFileStatusWithId> original = new ArrayList<>();
        if (childrenWithId != null) {
            for (HdfsFileStatusWithId child : childrenWithId) {
                getChildState(child.getFileStatus(), child, writeIdList, working, originalDirectories, original,
                        obsolete, bestBase, ignoreEmptyFiles, abortedDirectories, tblproperties, fs);
            }
        } else {
            List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, directory, hiddenFileFilter);
            for (FileStatus child : children) {
                getChildState(child, null, writeIdList, working, originalDirectories, original, obsolete,
                        bestBase, ignoreEmptyFiles, abortedDirectories, tblproperties, fs);
            }
        }

        // If we have a base, the original files are obsolete.
        if (bestBase.status != null) {
            // Add original files to obsolete list if any
            for (HdfsFileStatusWithId fswid : original) {
                obsolete.add(fswid.getFileStatus());
            }
            // Add original direcotries to obsolete list if any
            obsolete.addAll(originalDirectories);
            // remove the entries so we don't get confused later and think we should
            // use them.
            original.clear();
            originalDirectories.clear();
        } else {
            // Okay, we're going to need these originals.  Recurse through them and figure out what we
            // really need.
            for (FileStatus origDir : originalDirectories) {
                findOriginals(fs, origDir, original, useFileIds, ignoreEmptyFiles);
            }
        }

        Collections.sort(working);
        //so now, 'working' should be sorted like delta_5_20 delta_5_10 delta_11_20 delta_51_60 for example
        //and we want to end up with the best set containing all relevant data: delta_5_20 delta_51_60,
        //subject to list of 'exceptions' in 'writeIdList' (not show in above example).
        long current = bestBase.writeId;
        int lastStmtId = -1;
        ParsedDelta prev = null;
        for(ParsedDelta next: working) {
            if (next.maxWriteId > current) {
                // are any of the new transactions ones that we care about?
                if (writeIdList.isWriteIdRangeValid(current+1, next.maxWriteId) !=
                        ValidWriteIdList.RangeResponse.NONE) {
                    deltas.add(next);
                    current = next.maxWriteId;
                    lastStmtId = next.statementId;
                    prev = next;
                }
            }
            else if(next.maxWriteId == current && lastStmtId >= 0) {
                //make sure to get all deltas within a single transaction;  multi-statement txn
                //generate multiple delta files with the same txnId range
                //of course, if maxWriteId has already been minor compacted, all per statement deltas are obsolete
                deltas.add(next);
                prev = next;
            }
            else if (prev != null && next.maxWriteId == prev.maxWriteId
                    && next.minWriteId == prev.minWriteId
                    && next.statementId == prev.statementId) {
                // The 'next' parsedDelta may have everything equal to the 'prev' parsedDelta, except
                // the path. This may happen when we have split update and we have two types of delta
                // directories- 'delta_x_y' and 'delete_delta_x_y' for the SAME txn range.

                // Also note that any delete_deltas in between a given delta_x_y range would be made
                // obsolete. For example, a delta_30_50 would make delete_delta_40_40 obsolete.
                // This is valid because minor compaction always compacts the normal deltas and the delete
                // deltas for the same range. That is, if we had 3 directories, delta_30_30,
                // delete_delta_40_40 and delta_50_50, then running minor compaction would produce
                // delta_30_50 and delete_delta_30_50.

                deltas.add(next);
                prev = next;
            }
            else {
                obsolete.add(next.path);
            }
        }

        if(bestBase.oldestBase != null && bestBase.status == null) {
            /**
             * If here, it means there was a base_x (> 1 perhaps) but none were suitable for given
             * {@link writeIdList}.  Note that 'original' files are logically a base_Long.MIN_VALUE and thus
             * cannot have any data for an open txn.  We could check {@link deltas} has files to cover
             * [1,n] w/o gaps but this would almost never happen...*/
            long[] exceptions = writeIdList.getInvalidWriteIds();
            String minOpenWriteId = exceptions != null && exceptions.length > 0 ?
                    Long.toString(exceptions[0]) : "x";
            throw new IOException(ErrorMsg.ACID_NOT_ENOUGH_HISTORY.format(
                    Long.toString(writeIdList.getHighWatermark()),
                    minOpenWriteId, bestBase.oldestBase.toString()));
        }

        final Path base = bestBase.status == null ? null : bestBase.status.getPath();
        LOG.debug("in directory " + directory.toUri().toString() + " base = " + base + " deltas = " +
                deltas.size());
        /**
         * If this sort order is changed and there are tables that have been converted to transactional
         * and have had any update/delete/merge operations performed but not yet MAJOR compacted, it
         * may result in data loss since it may change how
         * {@link org.apache.hadoop.hive.ql.io.orc.OrcRawRecordMerger.OriginalReaderPair} assigns
         * {@link RecordIdentifier#rowId} for read (that have happened) and compaction (yet to happen).
         */
        Collections.sort(original, (HdfsFileStatusWithId o1, HdfsFileStatusWithId o2) -> {
            //this does "Path.uri.compareTo(that.uri)"
            return o1.getFileStatus().compareTo(o2.getFileStatus());
        });

        // Note: isRawFormat is invalid for non-ORC tables. It will always return true, so we're good.
        final boolean isBaseInRawFormat = base != null && AcidUtils.MetaDataFile.isRawFormat(base, fs);
        return new Directory() {

            @Override
            public Path getBaseDirectory() {
                return base;
            }
            @Override
            public boolean isBaseInRawFormat() {
                return isBaseInRawFormat;
            }

            @Override
            public List<HdfsFileStatusWithId> getOriginalFiles() {
                return original;
            }

            @Override
            public List<ParsedDelta> getCurrentDirectories() {
                return deltas;
            }

            @Override
            public List<FileStatus> getObsolete() {
                return obsolete;
            }

            @Override
            public List<FileStatus> getAbortedDirectories() {
                return abortedDirectories;
            }
        };
    }

    private static void getChildState(FileStatus child, HdfsFileStatusWithId childWithId,
            ValidWriteIdList writeIdList, List<ParsedDelta> working, List<FileStatus> originalDirectories,
            List<HdfsFileStatusWithId> original, List<FileStatus> obsolete, TxnBase bestBase,
            boolean ignoreEmptyFiles, List<FileStatus> aborted, Map<String, String> tblproperties,
            FileSystem fs) throws IOException {
        Path p = child.getPath();
        String fn = p.getName();
        if (!child.isDirectory()) {
            if (!ignoreEmptyFiles || child.getLen() != 0) {
                original.add(createOriginalObj(childWithId, child));
            }
            return;
        }
        if (fn.startsWith(BASE_PREFIX)) {
            long writeId = parseBase(p);
            if(bestBase.oldestBaseWriteId > writeId) {
                //keep track for error reporting
                bestBase.oldestBase = p;
                bestBase.oldestBaseWriteId = writeId;
            }
            if (bestBase.status == null) {
                if(isValidBase(writeId, writeIdList, p, fs)) {
                    bestBase.status = child;
                    bestBase.writeId = writeId;
                }
            } else if (bestBase.writeId < writeId) {
                if(isValidBase(writeId, writeIdList, p, fs)) {
                    obsolete.add(bestBase.status);
                    bestBase.status = child;
                    bestBase.writeId = writeId;
                }
            } else {
                obsolete.add(child);
            }
        } else if (fn.startsWith(DELTA_PREFIX) || fn.startsWith(DELETE_DELTA_PREFIX)) {
            String deltaPrefix = fn.startsWith(DELTA_PREFIX)  ? DELTA_PREFIX : DELETE_DELTA_PREFIX;
            ParsedDelta delta = parseDelta(child, deltaPrefix, fs);
            // Handle aborted deltas. Currently this can only happen for MM tables.
            if (tblproperties != null && isTransactionalTable(tblproperties) &&
                    ValidWriteIdList.RangeResponse.ALL == writeIdList.isWriteIdRangeAborted(
                            delta.minWriteId, delta.maxWriteId)) {
                aborted.add(child);
            }
            if (writeIdList.isWriteIdRangeValid(
                    delta.minWriteId, delta.maxWriteId) != ValidWriteIdList.RangeResponse.NONE) {
                working.add(delta);
            }
        } else {
            // This is just the directory.  We need to recurse and find the actual files.  But don't
            // do this until we have determined there is no base.  This saves time.  Plus,
            // it is possible that the cleaner is running and removing these original files,
            // in which case recursing through them could cause us to get an error.
            originalDirectories.add(child);
        }
    }

    /**
     * Get the write id from a base directory name.
     * @param path the base directory name
     * @return the maximum write id that is included
     */
    public static long parseBase(Path path) {
        String filename = path.getName();
        if (filename.startsWith(BASE_PREFIX)) {
            return Long.parseLong(filename.split("_")[1]);
        }
        throw new IllegalArgumentException(filename + " does not start with " +
                BASE_PREFIX);
    }

    private static ParsedDelta parseDelta(FileStatus path, String deltaPrefix, FileSystem fs)
            throws IOException {
        ParsedDelta p = parsedDelta(path.getPath(), deltaPrefix, fs);
        boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
        return new ParsedDelta(p.getMinWriteId(),
                p.getMaxWriteId(), path, p.statementId, isDeleteDelta, p.isRawFormat());
    }

    public static ParsedDelta parsedDelta(Path deltaDir, FileSystem fs) throws IOException {
        String deltaDirName = deltaDir.getName();
        if (deltaDirName.startsWith(DELETE_DELTA_PREFIX)) {
            return parsedDelta(deltaDir, DELETE_DELTA_PREFIX, fs);
        }
        return parsedDelta(deltaDir, DELTA_PREFIX, fs); // default prefix is delta_prefix
    }

    public static ParsedDelta parsedDelta(Path deltaDir, String deltaPrefix, FileSystem fs)
            throws IOException {
        String filename = deltaDir.getName();
        boolean isDeleteDelta = deltaPrefix.equals(DELETE_DELTA_PREFIX);
        if (filename.startsWith(deltaPrefix)) {
            //small optimization - delete delta can't be in raw format
            boolean isRawFormat = !isDeleteDelta && AcidUtils.MetaDataFile.isRawFormat(deltaDir, fs);
            String rest = filename.substring(deltaPrefix.length());
            int split = rest.indexOf('_');
            int split2 = rest.indexOf('_', split + 1);//may be -1 if no statementId
            long min = Long.parseLong(rest.substring(0, split));
            long max = split2 == -1 ?
                    Long.parseLong(rest.substring(split + 1)) :
                    Long.parseLong(rest.substring(split + 1, split2));
            if(split2 == -1) {
                return new ParsedDelta(min, max, null, isDeleteDelta, isRawFormat);
            }
            int statementId = Integer.parseInt(rest.substring(split2 + 1));
            return new ParsedDelta(min, max, null, statementId, isDeleteDelta, isRawFormat);
        }
        throw new IllegalArgumentException(deltaDir + " does not start with " +
                deltaPrefix);
    }

    /**
     * We can only use a 'base' if it doesn't have an open txn (from specific reader's point of view)
     * A 'base' with open txn in its range doesn't have 'enough history' info to produce a correct
     * snapshot for this reader.
     * Note that such base is NOT obsolete.  Obsolete files are those that are "covered" by other
     * files within the snapshot.
     * A base produced by Insert Overwrite is different.  Logically it's a delta file but one that
     * causes anything written previously is ignored (hence the overwrite).  In this case, base_x
     * is visible if writeid:x is committed for current reader.
     */
    private static boolean isValidBase(long baseWriteId, ValidWriteIdList writeIdList, Path baseDir,
            FileSystem fs) throws IOException {
        if(baseWriteId == Long.MIN_VALUE) {
            //such base is created by 1st compaction in case of non-acid to acid table conversion
            //By definition there are no open txns with id < 1.
            return true;
        }
        if(!AcidUtils.MetaDataFile.isCompacted(baseDir, fs)) {
            //this is the IOW case
            return writeIdList.isWriteIdValid(baseWriteId);
        }
        return writeIdList.isValidBase(baseWriteId);
    }

    /**
     * Find the original files (non-ACID layout) recursively under the partition directory.
     * @param fs the file system
     * @param stat the directory to add
     * @param original the list of original files
     * @throws IOException
     */
    private static void findOriginals(FileSystem fs, FileStatus stat,
            List<HdfsFileStatusWithId> original, Ref<Boolean> useFileIds, boolean ignoreEmptyFiles) throws IOException {
        assert stat.isDir();
        List<HdfsFileStatusWithId> childrenWithId = null;
        Boolean val = useFileIds.value;
        if (val == null || val) {
            try {
                childrenWithId = SHIMS.listLocatedHdfsStatus(fs, stat.getPath(), hiddenFileFilter);
                if (val == null) {
                    useFileIds.value = true;
                }
            } catch (Throwable t) {
                LOG.error("Failed to get files with ID; using regular API: " + t.getMessage());
                if (val == null && t instanceof UnsupportedOperationException) {
                    useFileIds.value = false;
                }
            }
        }
        if (childrenWithId != null) {
            for (HdfsFileStatusWithId child : childrenWithId) {
                if (child.getFileStatus().isDir()) {
                    findOriginals(fs, child.getFileStatus(), original, useFileIds, ignoreEmptyFiles);
                } else {
                    if(!ignoreEmptyFiles || child.getFileStatus().getLen() > 0) {
                        original.add(child);
                    }
                }
            }
        } else {
            List<FileStatus> children = HdfsUtils.listLocatedStatus(fs, stat.getPath(), hiddenFileFilter);
            for (FileStatus child : children) {
                if (child.isDir()) {
                    findOriginals(fs, child, original, useFileIds, ignoreEmptyFiles);
                } else {
                    if(!ignoreEmptyFiles || child.getLen() > 0) {
                        original.add(createOriginalObj(null, child));
                    }
                }
            }
        }
    }

    /** State class for getChildState; cannot modify 2 things in a method. */
    private static class TxnBase {
        private FileStatus status;
        private long writeId = 0;
        private long oldestBaseWriteId = Long.MAX_VALUE;
        private Path oldestBase = null;
    }

    public static final class ParsedDelta implements Comparable<ParsedDelta> {
        private final long minWriteId;
        private final long maxWriteId;
        private final FileStatus path;
        //-1 is for internal (getAcidState()) purposes and means the delta dir
        //had no statement ID
        private final int statementId;
        private final boolean isDeleteDelta; // records whether delta dir is of type 'delete_delta_x_y...'
        private final boolean isRawFormat;

        /**
         * for pre 1.3.x delta files
         */
        private ParsedDelta(long min, long max, FileStatus path, boolean isDeleteDelta,
                boolean isRawFormat) {
            this(min, max, path, -1, isDeleteDelta, isRawFormat);
        }
        private ParsedDelta(long min, long max, FileStatus path, int statementId,
                boolean isDeleteDelta, boolean isRawFormat) {
            this.minWriteId = min;
            this.maxWriteId = max;
            this.path = path;
            this.statementId = statementId;
            this.isDeleteDelta = isDeleteDelta;
            this.isRawFormat = isRawFormat;
            assert !isDeleteDelta || !isRawFormat : " deleteDelta should not be raw format";
        }

        public long getMinWriteId() {
            return minWriteId;
        }

        public long getMaxWriteId() {
            return maxWriteId;
        }

        public Path getPath() {
            return path.getPath();
        }

        public int getStatementId() {
            return statementId == -1 ? 0 : statementId;
        }

        public boolean isDeleteDelta() {
            return isDeleteDelta;
        }
        /**
         * Files w/o Acid meta columns embedded in the file. See {@link AcidBaseFileType#ORIGINAL_BASE}
         */
        public boolean isRawFormat() {
            return isRawFormat;
        }
        /**
         * Compactions (Major/Minor) merge deltas/bases but delete of old files
         * happens in a different process; thus it's possible to have bases/deltas with
         * overlapping writeId boundaries.  The sort order helps figure out the "best" set of files
         * to use to get data.
         * This sorts "wider" delta before "narrower" i.e. delta_5_20 sorts before delta_5_10 (and delta_11_20)
         */
        @Override
        public int compareTo(ParsedDelta parsedDelta) {
            if (minWriteId != parsedDelta.minWriteId) {
                if (minWriteId < parsedDelta.minWriteId) {
                    return -1;
                } else {
                    return 1;
                }
            } else if (maxWriteId != parsedDelta.maxWriteId) {
                if (maxWriteId < parsedDelta.maxWriteId) {
                    return 1;
                } else {
                    return -1;
                }
            }
            else if(statementId != parsedDelta.statementId) {
                /**
                 * We want deltas after minor compaction (w/o statementId) to sort
                 * earlier so that getAcidState() considers compacted files (into larger ones) obsolete
                 * Before compaction, include deltas with all statementIds for a given writeId
                 * in a {@link org.apache.hadoop.hive.ql.io.AcidUtils.Directory}
                 */
                if(statementId < parsedDelta.statementId) {
                    return -1;
                }
                else {
                    return 1;
                }
            }
            else {
                return path.compareTo(parsedDelta.path);
            }
        }
    }

    public static interface Directory {

        /**
         * Get the base directory.
         * @return the base directory to read
         */
        Path getBaseDirectory();
        boolean isBaseInRawFormat();

        /**
         * Get the list of original files.  Not {@code null}.  Must be sorted.
         * @return the list of original files (eg. 000000_0)
         */
        List<HdfsFileStatusWithId> getOriginalFiles();

        /**
         * Get the list of base and delta directories that are valid and not
         * obsolete.  Not {@code null}.  List must be sorted in a specific way.
         * See {@link org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta#compareTo(org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta)}
         * for details.
         * @return the minimal list of current directories
         */
        List<ParsedDelta> getCurrentDirectories();

        /**
         * Get the list of obsolete directories. After filtering out bases and
         * deltas that are not selected by the valid transaction/write ids list, return the
         * list of original files, bases, and deltas that have been replaced by
         * more up to date ones.  Not {@code null}.
         */
        List<FileStatus> getObsolete();

        /**
         * Get the list of directories that has nothing but aborted transactions.
         * @return the list of aborted directories
         */
        List<FileStatus> getAbortedDirectories();
    }
}
