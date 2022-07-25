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
package io.trino.plugin.hive;

import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.orc.OrcWriter.OrcOperation.DELETE;
import static io.trino.orc.OrcWriter.OrcOperation.INSERT;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.acid.AcidSchema.ACID_COLUMN_NAMES;
import static io.trino.plugin.hive.acid.AcidSchema.createAcidSchema;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deleteDeltaSubdir;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deltaSubdir;

public abstract class AbstractHiveAcidWriters
{
    public static final Block DELETE_OPERATION_BLOCK = nativeValueToBlock(INTEGER, Long.valueOf(DELETE.getOperationNumber()));
    public static final Block INSERT_OPERATION_BLOCK = nativeValueToBlock(INTEGER, Long.valueOf(INSERT.getOperationNumber()));

    // The bucketPath looks like .../delta_nnnnnnn_mmmmmmm_ssss/bucket_bbbbb(_aaaa)?
    public static final Pattern BUCKET_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>delta_[\\d]+_[\\d]+)_(?<statementId>[\\d]+)/(?<filenameBase>bucket_(?<bucketNumber>[\\d]+))(?<attemptId>_[\\d]+)?$");
    // The original file path looks like .../nnnnnnn_m(_copy_ccc)?
    public static final Pattern ORIGINAL_FILE_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<filename>(?<bucketNumber>[\\d]+)_(?<rest>.*)?)$");
    // After compaction, the bucketPath looks like .../base_nnnnnnn(_vmmmmmmm)?/bucket_bbbbb(_aaaa)?
    public static final Pattern BASE_PATH_MATCHER = Pattern.compile("(?s)(?<rootDir>.*)/(?<dirStart>base_[-]?[\\d]+(_v[\\d]+)?)/(?<filenameBase>bucket_(?<bucketNumber>[\\d]+))(?<attemptId>_[\\d]+)?$");

    protected final AcidTransaction transaction;
    protected final OptionalInt bucketNumber;
    protected final int statementId;
    private final OrcFileWriterFactory orcFileWriterFactory;
    private final Configuration configuration;
    protected final ConnectorSession session;
    protected final HiveType hiveRowType;
    private final Properties hiveAcidSchema;
    protected final Path deleteDeltaDirectory;
    private final String bucketFilename;
    protected Optional<Path> deltaDirectory;

    protected Optional<FileWriter> deleteFileWriter = Optional.empty();
    protected Optional<FileWriter> insertFileWriter = Optional.empty();

    public AbstractHiveAcidWriters(
            AcidTransaction transaction,
            int statementId,
            OptionalInt bucketNumber,
            Path bucketPath,
            boolean originalFile,
            OrcFileWriterFactory orcFileWriterFactory,
            Configuration configuration,
            ConnectorSession session,
            HiveType hiveRowType,
            AcidOperation updateKind)
    {
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.statementId = statementId;
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.session = requireNonNull(session, "session is null");
        checkArgument(transaction.isTransactional(), "Not in a transaction: %s", transaction);
        this.hiveRowType = requireNonNull(hiveRowType, "hiveRowType is null");
        this.hiveAcidSchema = createAcidSchema(hiveRowType);
        requireNonNull(bucketPath, "bucketPath is null");
        Matcher matcher;
        if (originalFile) {
            matcher = ORIGINAL_FILE_PATH_MATCHER.matcher(bucketPath.toString());
            checkArgument(matcher.matches(), "Original file bucketPath doesn't have the required format: %s", bucketPath);
            this.bucketFilename = format("bucket_%05d", bucketNumber.isEmpty() ? 0 : bucketNumber.getAsInt());
        }
        else {
            matcher = BASE_PATH_MATCHER.matcher(bucketPath.toString());
            if (matcher.matches()) {
                this.bucketFilename = matcher.group("filenameBase");
            }
            else {
                matcher = BUCKET_PATH_MATCHER.matcher(bucketPath.toString());
                checkArgument(matcher.matches(), "bucketPath doesn't have the required format: %s", bucketPath);
                this.bucketFilename = matcher.group("filenameBase");
            }
        }
        long writeId = transaction.getWriteId();
        this.deleteDeltaDirectory = new Path(format("%s/%s", matcher.group("rootDir"), deleteDeltaSubdir(writeId, writeId, statementId)));
        if (updateKind == AcidOperation.UPDATE) {
            this.deltaDirectory = Optional.of(new Path(format("%s/%s", matcher.group("rootDir"), deltaSubdir(writeId, writeId, statementId))));
        }
        else {
            this.deltaDirectory = Optional.empty();
        }
    }

    protected void lazyInitializeDeleteFileWriter()
    {
        if (deleteFileWriter.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            deleteFileWriter = orcFileWriterFactory.createFileWriter(
                    new Path(format("%s/%s", deleteDeltaDirectory, bucketFilename)),
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    schemaCopy,
                    toJobConf(configuration),
                    session,
                    bucketNumber,
                    transaction,
                    true,
                    WriterKind.DELETE);
        }
    }

    protected void lazyInitializeInsertFileWriter()
    {
        if (insertFileWriter.isEmpty()) {
            Properties schemaCopy = new Properties();
            schemaCopy.putAll(hiveAcidSchema);
            Path deltaDir = deltaDirectory.orElseThrow(() -> new IllegalArgumentException("deltaDirectory not present"));
            insertFileWriter = orcFileWriterFactory.createFileWriter(
                    new Path(format("%s/%s", deltaDir, bucketFilename)),
                    ACID_COLUMN_NAMES,
                    fromHiveStorageFormat(ORC),
                    schemaCopy,
                    toJobConf(configuration),
                    session,
                    bucketNumber,
                    transaction,
                    true,
                    WriterKind.INSERT);
        }
    }
}
