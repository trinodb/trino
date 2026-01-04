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
package io.trino.plugin.iceberg;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Splitter.MapSplitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.TableInfo;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.filter.UtcConstraintExtractor;
import io.trino.plugin.base.projection.ApplyProjectionUtil;
import io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;
import io.trino.plugin.hive.HiveCompressionCodec;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.HiveWrittenPartitions;
import io.trino.plugin.iceberg.aggregation.DataSketchStateSerializer;
import io.trino.plugin.iceberg.aggregation.IcebergThetaSketchForStats;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.functions.IcebergFunctionProvider;
import io.trino.plugin.iceberg.procedure.IcebergAddFilesFromTableHandle;
import io.trino.plugin.iceberg.procedure.IcebergAddFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergDropExtendedStatsHandle;
import io.trino.plugin.iceberg.procedure.IcebergExpireSnapshotsHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeHandle;
import io.trino.plugin.iceberg.procedure.IcebergOptimizeManifestsHandle;
import io.trino.plugin.iceberg.procedure.IcebergRemoveOrphanFilesHandle;
import io.trino.plugin.iceberg.procedure.IcebergRollbackToSnapshotHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableExecuteHandle;
import io.trino.plugin.iceberg.procedure.IcebergTableProcedureId;
import io.trino.plugin.iceberg.procedure.MigrationUtils.RecursiveDirectory;
import io.trino.plugin.iceberg.system.AllManifestsTable;
import io.trino.plugin.iceberg.system.EntriesTable;
import io.trino.plugin.iceberg.system.FilesTable;
import io.trino.plugin.iceberg.system.HistoryTable;
import io.trino.plugin.iceberg.system.ManifestsTable;
import io.trino.plugin.iceberg.system.MetadataLogEntriesTable;
import io.trino.plugin.iceberg.system.PartitionsTable;
import io.trino.plugin.iceberg.system.PropertiesTable;
import io.trino.plugin.iceberg.system.RefsTable;
import io.trino.plugin.iceberg.system.SnapshotsTable;
import io.trino.plugin.iceberg.util.DataFileWithDeleteFiles;
import io.trino.spi.ErrorCode;
import io.trino.spi.Page;
import io.trino.spi.RefreshType;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.BeginTableExecuteResult;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnPosition;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorAnalyzeMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableExecuteHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DiscretePredicates;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.RelationCommentMetadata;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IcebergManifestUtils;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.UpdateStatistics;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Sets.difference;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.filesystem.Locations.isS3Tables;
import static io.trino.plugin.base.filter.UtcConstraintExtractor.extractTupleDomain;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.extractSupportedProjectedColumns;
import static io.trino.plugin.base.projection.ApplyProjectionUtil.replaceWithNewVariables;
import static io.trino.plugin.base.util.ExecutorUtil.processWithAdditionalThreads;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveMetadata.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.ExpressionConverter.isConvertibleToIcebergExpression;
import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergAnalyzeProperties.getColumnNames;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_PARTITION_DATA;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_PARTITION_SPEC_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_MERGE_ROW_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.TRINO_ROW_ID_NAME;
import static io.trino.plugin.iceberg.IcebergColumnHandle.fileModifiedTimeColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.partitionColumnHandle;
import static io.trino.plugin.iceberg.IcebergColumnHandle.pathColumnHandle;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_METADATA;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_UNSUPPORTED_VIEW_DIALECT;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_MODIFIED_TIME;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.FILE_PATH;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.PARTITION;
import static io.trino.plugin.iceberg.IcebergMetadataColumn.isMetadataColumnId;
import static io.trino.plugin.iceberg.IcebergPartitionFunction.Transform.BUCKET;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getExpireSnapshotMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getHiveCatalogName;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getQueryPartitionFilterRequiredSchemas;
import static io.trino.plugin.iceberg.IcebergSessionProperties.getRemoveOrphanFilesMinRetention;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isBucketExecutionEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isCollectExtendedStatisticsOnWrite;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isExtendedStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isFileBasedConflictDetectionEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isIncrementalRefreshEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isMergeManifestsOnWrite;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isProjectionPushdownEnabled;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isQueryPartitionFilterRequired;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isStatisticsEnabled;
import static io.trino.plugin.iceberg.IcebergTableName.isDataTable;
import static io.trino.plugin.iceberg.IcebergTableName.isIcebergTableName;
import static io.trino.plugin.iceberg.IcebergTableName.isMaterializedViewStorage;
import static io.trino.plugin.iceberg.IcebergTableName.tableNameFrom;
import static io.trino.plugin.iceberg.IcebergTableProperties.COMPRESSION_CODEC;
import static io.trino.plugin.iceberg.IcebergTableProperties.DATA_LOCATION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.DELETE_AFTER_COMMIT_ENABLED;
import static io.trino.plugin.iceberg.IcebergTableProperties.EXTRA_PROPERTIES_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FILE_FORMAT_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.FORMAT_VERSION_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.MAX_COMMIT_RETRY;
import static io.trino.plugin.iceberg.IcebergTableProperties.MAX_PREVIOUS_VERSIONS;
import static io.trino.plugin.iceberg.IcebergTableProperties.OBJECT_STORE_LAYOUT_ENABLED_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.ORC_BLOOM_FILTER_COLUMNS_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.PARTITIONING_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergTableProperties.getTableLocation;
import static io.trino.plugin.iceberg.IcebergTableProperties.validateCompression;
import static io.trino.plugin.iceberg.IcebergUtil.buildPath;
import static io.trino.plugin.iceberg.IcebergUtil.canEnforceColumnConstraintInSpecs;
import static io.trino.plugin.iceberg.IcebergUtil.checkFormatForProperty;
import static io.trino.plugin.iceberg.IcebergUtil.commit;
import static io.trino.plugin.iceberg.IcebergUtil.createColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.trino.plugin.iceberg.IcebergUtil.fileName;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshot;
import static io.trino.plugin.iceberg.IcebergUtil.firstSnapshotAfter;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnMetadatas;
import static io.trino.plugin.iceberg.IcebergUtil.getCompressionPropertyName;
import static io.trino.plugin.iceberg.IcebergUtil.getFileFormat;
import static io.trino.plugin.iceberg.IcebergUtil.getHiveCompressionCodec;
import static io.trino.plugin.iceberg.IcebergUtil.getIcebergTableProperties;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionValues;
import static io.trino.plugin.iceberg.IcebergUtil.getProjectedColumns;
import static io.trino.plugin.iceberg.IcebergUtil.getSnapshotIdAsOfTime;
import static io.trino.plugin.iceberg.IcebergUtil.getTableComment;
import static io.trino.plugin.iceberg.IcebergUtil.getTopLevelColumns;
import static io.trino.plugin.iceberg.IcebergUtil.newCreateTableTransaction;
import static io.trino.plugin.iceberg.IcebergUtil.readerForManifest;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromMetadata;
import static io.trino.plugin.iceberg.IcebergUtil.validateOrcBloomFilterColumns;
import static io.trino.plugin.iceberg.IcebergUtil.validateParquetBloomFilterColumns;
import static io.trino.plugin.iceberg.IcebergUtil.verifyExtraProperties;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.SortFieldUtils.parseSortFields;
import static io.trino.plugin.iceberg.StructLikeWrapperWithFieldIdToIndex.createStructLikeWrapper;
import static io.trino.plugin.iceberg.TableStatisticsReader.readNdvs;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.INCREMENTAL_UPDATE;
import static io.trino.plugin.iceberg.TableStatisticsWriter.StatsUpdateMode.REPLACE;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ADD_FILES_FROM_TABLE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.DROP_EXTENDED_STATS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.EXPIRE_SNAPSHOTS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.OPTIMIZE_MANIFESTS;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.REMOVE_ORPHAN_FILES;
import static io.trino.plugin.iceberg.procedure.IcebergTableProcedureId.ROLLBACK_TO_SNAPSHOT;
import static io.trino.plugin.iceberg.procedure.MigrationUtils.addFiles;
import static io.trino.plugin.iceberg.procedure.MigrationUtils.addFilesFromTable;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.spi.connector.RowChangeParadigm.DELETE_ROW_AND_INSERT_ROW;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Math.floorDiv;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;
import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.apache.iceberg.ReachableFileUtil.metadataFileLocations;
import static org.apache.iceberg.ReachableFileUtil.statisticsFilesLocations;
import static org.apache.iceberg.SnapshotSummary.DELETED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.REMOVED_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.DELETE_ISOLATION_LEVEL_DEFAULT;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP_DEFAULT;
import static org.apache.iceberg.TableProperties.OBJECT_STORE_ENABLED;
import static org.apache.iceberg.TableProperties.ORC_BLOOM_FILTER_COLUMNS;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;
import static org.apache.iceberg.TableProperties.WRITE_DATA_LOCATION;
import static org.apache.iceberg.TableProperties.WRITE_LOCATION_PROVIDER_IMPL;
import static org.apache.iceberg.TableUtil.formatVersion;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.types.TypeUtil.indexParents;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;
import static org.apache.iceberg.util.PropertyUtil.propertyAsInt;
import static org.apache.iceberg.util.SnapshotUtil.schemaFor;

public class IcebergMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(IcebergMetadata.class);
    private static final int OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final int CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION = 2;
    private static final String RETENTION_THRESHOLD = "retention_threshold";
    private static final String UNKNOWN_SNAPSHOT_TOKEN = "UNKNOWN";
    public static final Set<String> UPDATABLE_TABLE_PROPERTIES = ImmutableSet.<String>builder()
            .add(EXTRA_PROPERTIES_PROPERTY)
            .add(FILE_FORMAT_PROPERTY)
            .add(FORMAT_VERSION_PROPERTY)
            .add(COMPRESSION_CODEC)
            .add(MAX_COMMIT_RETRY)
            .add(DELETE_AFTER_COMMIT_ENABLED)
            .add(MAX_PREVIOUS_VERSIONS)
            .add(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
            .add(DATA_LOCATION_PROPERTY)
            .add(ORC_BLOOM_FILTER_COLUMNS_PROPERTY)
            .add(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY)
            .add(PARTITIONING_PROPERTY)
            .add(SORTED_BY_PROPERTY)
            .add(IcebergTableProperties.WRITE_DELETE_MODE)
            .add(IcebergTableProperties.WRITE_UPDATE_MODE)
            .add(IcebergTableProperties.WRITE_MERGE_MODE)
            .build();
    private static final String SYSTEM_SCHEMA = "system";

    public static final String NUMBER_OF_DISTINCT_VALUES_NAME = "NUMBER_OF_DISTINCT_VALUES";
    private static final FunctionName NUMBER_OF_DISTINCT_VALUES_FUNCTION = new FunctionName(IcebergThetaSketchForStats.NAME);

    private static final Integer DELETE_BATCH_SIZE = 1000;
    public static final int GET_METADATA_BATCH_SIZE = 1000;
    private static final MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=");

    private static final String DEPENDS_ON_TABLES = "dependsOnTables";
    private static final String DEPENDS_ON_TABLE_FUNCTIONS = "dependsOnTableFunctions";
    // Value should be ISO-8601 formatted time instant
    private static final String TRINO_QUERY_START_TIME = "trino-query-start-time";

    private final TypeManager typeManager;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalog catalog;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final TableStatisticsReader tableStatisticsReader;
    private final TableStatisticsWriter tableStatisticsWriter;
    private final Optional<HiveMetastoreFactory> metastoreFactory;
    private final boolean addFilesProcedureEnabled;
    private final Predicate<String> allowedExtraProperties;
    private final ExecutorService icebergScanExecutor;
    private final Executor metadataFetchingExecutor;
    private final ExecutorService icebergPlanningExecutor;
    private final ExecutorService icebergFileDeleteExecutor;
    private final IcebergPageSourceProvider pageSourceProvider;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final Map<IcebergTableHandle, AtomicReference<TableStatistics>> tableStatisticsCache = new ConcurrentHashMap<>();

    private Transaction transaction;
    private Optional<Long> fromSnapshotForRefresh = Optional.empty();

    public IcebergMetadata(
            TypeManager typeManager,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalog catalog,
            IcebergFileSystemFactory fileSystemFactory,
            TableStatisticsReader tableStatisticsReader,
            TableStatisticsWriter tableStatisticsWriter,
            Optional<HiveMetastoreFactory> metastoreFactory,
            boolean addFilesProcedureEnabled,
            Predicate<String> allowedExtraProperties,
            ExecutorService icebergScanExecutor,
            Executor metadataFetchingExecutor,
            ExecutorService icebergPlanningExecutor,
            ExecutorService icebergFileDeleteExecutor,
            IcebergPageSourceProvider pageSourceProvider,
            IcebergFileWriterFactory fileWriterFactory)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableStatisticsReader = requireNonNull(tableStatisticsReader, "tableStatisticsReader is null");
        this.tableStatisticsWriter = requireNonNull(tableStatisticsWriter, "tableStatisticsWriter is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.addFilesProcedureEnabled = addFilesProcedureEnabled;
        this.allowedExtraProperties = requireNonNull(allowedExtraProperties, "allowedExtraProperties is null");
        this.icebergScanExecutor = requireNonNull(icebergScanExecutor, "icebergScanExecutor is null");
        this.metadataFetchingExecutor = requireNonNull(metadataFetchingExecutor, "metadataFetchingExecutor is null");
        this.icebergPlanningExecutor = requireNonNull(icebergPlanningExecutor, "icebergPlanningExecutor is null");
        this.icebergFileDeleteExecutor = requireNonNull(icebergFileDeleteExecutor, "icebergFileDeleteExecutor is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
    }

    @Override
    public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName)
    {
        return schemaName.equals(SYSTEM_SCHEMA) ? IcebergFunctionProvider.FUNCTIONS : List.of();
    }

    @Override
    public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name)
    {
        if (!name.getSchemaName().equals(SYSTEM_SCHEMA)) {
            return List.of();
        }
        return IcebergFunctionProvider.FUNCTIONS.stream()
                .filter(function -> function.getCanonicalName().equals(name.getFunctionName()))
                .toList();
    }

    @Override
    public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId)
    {
        return IcebergFunctionProvider.FUNCTIONS.stream()
                .filter(function -> function.getFunctionId().equals(functionId))
                .findFirst()
                .orElseThrow();
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName)
    {
        return catalog.namespaceExists(session, schemaName);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return catalog.listNamespaces(session);
    }

    @Override
    public Map<String, Object> getSchemaProperties(ConnectorSession session, String schemaName)
    {
        return catalog.loadNamespaceMetadata(session, schemaName);
    }

    @Override
    public Optional<TrinoPrincipal> getSchemaOwner(ConnectorSession session, String schemaName)
    {
        return catalog.getNamespacePrincipal(session, schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName tableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "Read table with start version is not supported");
        }

        if (!isIcebergTableName(tableName.getTableName())) {
            return null;
        }

        if (isMaterializedViewStorage(tableName.getTableName())) {
            verify(endVersion.isEmpty(), "Materialized views do not support versioned queries");

            SchemaTableName materializedViewName = new SchemaTableName(tableName.getSchemaName(), tableNameFrom(tableName.getTableName()));
            if (getMaterializedView(session, materializedViewName).isEmpty()) {
                throw new TableNotFoundException(tableName);
            }

            BaseTable storageTable = catalog.getMaterializedViewStorageTable(session, materializedViewName)
                    .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, "Storage table metadata not found for materialized view " + tableName));

            return tableHandleForCurrentSnapshot(session, tableName, storageTable);
        }

        if (!isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }

        BaseTable table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), tableName.getTableName()));
        }
        catch (TableNotFoundException e) {
            return null;
        }
        catch (TrinoException e) {
            ErrorCode errorCode = e.getErrorCode();
            if (errorCode.equals(ICEBERG_MISSING_METADATA.toErrorCode())
                    || errorCode.equals(ICEBERG_INVALID_METADATA.toErrorCode())) {
                return new CorruptedIcebergTableHandle(tableName, e);
            }
            throw e;
        }

        if (endVersion.isPresent()) {
            long snapshotId = getSnapshotIdFromVersion(session, table, endVersion.get());
            return tableHandleForSnapshot(
                    session,
                    tableName,
                    table,
                    Optional.of(snapshotId),
                    schemaFor(table, snapshotId),
                    Optional.empty());
        }
        return tableHandleForCurrentSnapshot(session, tableName, table);
    }

    private IcebergTableHandle tableHandleForCurrentSnapshot(ConnectorSession session, SchemaTableName tableName, BaseTable table)
    {
        return tableHandleForSnapshot(
                session,
                tableName,
                table,
                Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId),
                table.schema(),
                Optional.of(table.spec()));
    }

    private IcebergTableHandle tableHandleForSnapshot(
            ConnectorSession session,
            SchemaTableName tableName,
            BaseTable table,
            Optional<Long> tableSnapshotId,
            Schema tableSchema,
            Optional<PartitionSpec> partitionSpec)
    {
        Map<String, String> tableProperties = table.properties();
        return new IcebergTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                DATA,
                tableSnapshotId,
                SchemaParser.toJson(tableSchema),
                partitionSpec.map(PartitionSpecParser::toJson),
                formatVersion(table),
                TupleDomain.all(),
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.ofNullable(tableProperties.get(TableProperties.DEFAULT_NAME_MAPPING)),
                table.location(),
                table.properties(),
                getTablePartitioning(session, table),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false),
                Optional.empty());
    }

    private Optional<IcebergTablePartitioning> getTablePartitioning(ConnectorSession session, Table icebergTable)
    {
        if (!isBucketExecutionEnabled(session) || icebergTable.specs().size() != 1) {
            return Optional.empty();
        }
        PartitionSpec partitionSpec = icebergTable.spec();
        if (partitionSpec.fields().isEmpty()) {
            return Optional.empty();
        }

        IcebergPartitioningHandle partitioningHandle = IcebergPartitioningHandle.create(partitionSpec, typeManager, List.of());

        Map<Integer, IcebergColumnHandle> partitionColumnById = getPartitionColumns(icebergTable, typeManager).stream()
                .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));
        List<IcebergColumnHandle> partitionColumns = partitionSpec.fields().stream()
                .map(PartitionField::sourceId)
                .distinct()
                .sorted()
                .map(partitionColumnById::get)
                .collect(toImmutableList());

        // Partitioning is only activated if it is actually necessary for the query.
        // This happens in applyPartitioning
        return Optional.of(new IcebergTablePartitioning(
                false,
                partitioningHandle,
                partitionColumns,
                IntStream.range(0, partitioningHandle.partitionFunctions().size()).boxed().collect(toImmutableList())));
    }

    private static long getSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version)
    {
        io.trino.spi.type.Type versionType = version.getVersionType();
        return switch (version.getPointerType()) {
            case TEMPORAL -> getTemporalSnapshotIdFromVersion(session, table, version, versionType);
            case TARGET_ID -> getTargetSnapshotIdFromVersion(table, version, versionType);
        };
    }

    private static long getTargetSnapshotIdFromVersion(Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        long snapshotId;
        if (versionType == BIGINT) {
            snapshotId = (long) version.getVersion();
        }
        else if (versionType instanceof VarcharType) {
            String refName = ((Slice) version.getVersion()).toStringUtf8();
            SnapshotRef ref = table.refs().get(refName);
            if (ref == null) {
                throw new TrinoException(INVALID_ARGUMENTS, "Cannot find snapshot with reference name: " + refName);
            }
            snapshotId = ref.snapshotId();
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type for table version: " + versionType.getDisplayName());
        }

        if (table.snapshot(snapshotId) == null) {
            throw new TrinoException(INVALID_ARGUMENTS, "Iceberg snapshot ID does not exists: " + snapshotId);
        }
        return snapshotId;
    }

    private static long getTemporalSnapshotIdFromVersion(ConnectorSession session, Table table, ConnectorTableVersion version, io.trino.spi.type.Type versionType)
    {
        if (versionType.equals(DATE)) {
            // Retrieve the latest snapshot made before or at the beginning of the day of the specified date in the session's time zone
            long epochMillis = LocalDate.ofEpochDay((Long) version.getVersion())
                    .atStartOfDay()
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampType timestampVersionType) {
            long epochMicrosUtc = timestampVersionType.isShort()
                    ? (long) version.getVersion()
                    : ((LongTimestamp) version.getVersion()).getEpochMicros();
            long epochMillisUtc = floorDiv(epochMicrosUtc, MICROSECONDS_PER_MILLISECOND);
            long epochMillis = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillisUtc), ZoneOffset.UTC)
                    .atZone(session.getTimeZoneKey().getZoneId())
                    .toInstant()
                    .toEpochMilli();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        if (versionType instanceof TimestampWithTimeZoneType timeZonedVersionType) {
            long epochMillis = timeZonedVersionType.isShort()
                    ? unpackMillisUtc((long) version.getVersion())
                    : ((LongTimestampWithTimeZone) version.getVersion()).getEpochMillis();
            return getSnapshotIdAsOfTime(table, epochMillis);
        }
        throw new TrinoException(NOT_SUPPORTED, "Unsupported type for temporal table version: " + versionType.getDisplayName());
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return getRawSystemTable(session, tableName)
                .map(systemTable -> new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
    }

    private Optional<SystemTable> getRawSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (!isIcebergTableName(tableName.getTableName()) || isDataTable(tableName.getTableName()) || isMaterializedViewStorage(tableName.getTableName())) {
            return Optional.empty();
        }

        // Only when dealing with an actual system table proceed to retrieve the base table for the system table
        String name = tableNameFrom(tableName.getTableName());
        BaseTable table;
        try {
            table = catalog.loadTable(session, new SchemaTableName(tableName.getSchemaName(), name));
        }
        catch (TableNotFoundException e) {
            return Optional.empty();
        }
        catch (UnknownTableTypeException e) {
            // avoid dealing with non Iceberg tables
            return Optional.empty();
        }

        TableType tableType = IcebergTableName.tableTypeFrom(tableName.getTableName());
        return switch (tableType) {
            case DATA, MATERIALIZED_VIEW_STORAGE -> throw new VerifyException("Unexpected table type: " + tableType); // Handled above.
            case HISTORY -> Optional.of(new HistoryTable(tableName, table));
            case METADATA_LOG_ENTRIES -> Optional.of(new MetadataLogEntriesTable(tableName, table, icebergScanExecutor));
            case SNAPSHOTS -> Optional.of(new SnapshotsTable(tableName, typeManager, table, icebergScanExecutor));
            case PARTITIONS -> Optional.of(new PartitionsTable(tableName, typeManager, table, getCurrentSnapshotId(table), icebergScanExecutor));
            case ALL_MANIFESTS -> Optional.of(new AllManifestsTable(tableName, table, icebergScanExecutor));
            case MANIFESTS -> Optional.of(new ManifestsTable(tableName, table, getCurrentSnapshotId(table)));
            case FILES -> Optional.of(new FilesTable(tableName, typeManager, table, getCurrentSnapshotId(table)));
            case ALL_ENTRIES -> Optional.of(new EntriesTable(typeManager, tableName, table, ALL_ENTRIES, icebergScanExecutor));
            case ENTRIES -> Optional.of(new EntriesTable(typeManager, tableName, table, ENTRIES, icebergScanExecutor));
            case PROPERTIES -> Optional.of(new PropertiesTable(tableName, table));
            case REFS -> Optional.of(new RefsTable(tableName, table, icebergScanExecutor));
        };
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;

        if (table.getSnapshotId().isEmpty()) {
            // A table with missing snapshot id produces no splits, so we optimize here by returning
            // TupleDomain.none() as the predicate
            return new ConnectorTableProperties(TupleDomain.none(), Optional.empty(), Optional.empty(), ImmutableList.of());
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        // Extract identity partition fields that are present in all partition specs, for creating the discrete predicates.
        Set<Integer> partitionSourceIds = identityPartitionColumnsInAllSpecs(icebergTable);

        TupleDomain<IcebergColumnHandle> enforcedPredicate = table.getEnforcedPredicate();

        DiscretePredicates discretePredicates = null;
        if (!partitionSourceIds.isEmpty()) {
            // Extract identity partition columns
            Map<Integer, IcebergColumnHandle> columns = getProjectedColumns(icebergTable.schema(), typeManager, partitionSourceIds).stream()
                    .collect(toImmutableMap(IcebergColumnHandle::getId, identity()));

            Supplier<Map<StructLikeWrapperWithFieldIdToIndex, PartitionSpec>> lazyUniquePartitions = Suppliers.memoize(() -> {
                TableScan tableScan = icebergTable.newScan()
                        .useSnapshot(table.getSnapshotId().get())
                        .filter(toIcebergExpression(enforcedPredicate))
                        .planWith(icebergPlanningExecutor);

                try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
                    Map<StructLikeWrapperWithFieldIdToIndex, PartitionSpec> partitions = new HashMap<>();
                    for (FileScanTask fileScanTask : fileScanTasks) {
                        StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = createStructLikeWrapper(fileScanTask);
                        partitions.putIfAbsent(structLikeWrapperWithFieldIdToIndex, fileScanTask.spec());
                    }
                    return partitions;
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            Iterable<TupleDomain<ColumnHandle>> discreteTupleDomain = Iterables.transform(
                    () -> lazyUniquePartitions.get().entrySet().iterator(),
                    entry -> {
                        // Extract partition values
                        Map<Integer, Optional<String>> partitionColumnValueStrings = getPartitionKeys(entry.getKey().getStructLikeWrapper().get(), entry.getValue());
                        Map<ColumnHandle, NullableValue> partitionValues = partitionSourceIds.stream()
                                .filter(partitionColumnValueStrings::containsKey)
                                .collect(toImmutableMap(
                                        columns::get,
                                        columnId -> {
                                            IcebergColumnHandle column = columns.get(columnId);
                                            Object prestoValue = deserializePartitionValue(
                                                    column.getType(),
                                                    partitionColumnValueStrings.get(columnId).orElse(null),
                                                    column.getName());

                                            return new NullableValue(column.getType(), prestoValue);
                                        }));

                        return TupleDomain.fromFixedValues(partitionValues);
                    });

            discretePredicates = new DiscretePredicates(
                    columns.values().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()),
                    discreteTupleDomain);
        }

        return new ConnectorTableProperties(
                // Using the predicate here directly avoids eagerly loading all partition values. Logically, this
                // still keeps predicate and discretePredicates evaluation the same on every row of the table. This
                // can be further optimized by intersecting with partition values at the cost of iterating
                // over all tableScan.planFiles() and caching partition values in table handle.
                enforcedPredicate.transformKeys(ColumnHandle.class::cast),
                table.getTablePartitioning().flatMap(IcebergTablePartitioning::toConnectorTablePartitioning),
                Optional.ofNullable(discretePredicates),
                // todo support sorting properties
                ImmutableList.of());
    }

    @Override
    public Optional<ConnectorTableHandle> applyPartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<ConnectorPartitioningHandle> partitioningHandle, List<ColumnHandle> partitioningColumns)
    {
        IcebergTableHandle icebergTableHandle = checkValidTableHandle(tableHandle);
        if (icebergTableHandle.getPartitionSpecJson().isEmpty()) {
            return Optional.empty();
        }

        Optional<IcebergTablePartitioning> connectorTablePartitioning = icebergTableHandle.getTablePartitioning();
        if (connectorTablePartitioning.isEmpty()) {
            return Optional.empty();
        }
        IcebergTablePartitioning tablePartitioning = connectorTablePartitioning.get();

        // Check if the table can be partitioned on the requested columns
        if (!new HashSet<>(tablePartitioning.partitioningColumns()).containsAll(partitioningColumns)) {
            return Optional.empty();
        }

        Map<ColumnHandle, Integer> newPartitioningColumnIndex = IntStream.range(0, partitioningColumns.size()).boxed()
                .collect(toImmutableMap(partitioningColumns::get, identity()));
        ImmutableList.Builder<IcebergPartitionFunction> newPartitionFunctions = ImmutableList.builder();
        ImmutableList.Builder<Integer> newPartitionStructFields = ImmutableList.builder();
        for (int functionIndex = 0; functionIndex < tablePartitioning.partitioningHandle().partitionFunctions().size(); functionIndex++) {
            IcebergPartitionFunction function = tablePartitioning.partitioningHandle().partitionFunctions().get(functionIndex);
            int oldColumnIndex = function.dataPath().getFirst();
            Integer newColumnIndex = newPartitioningColumnIndex.get(tablePartitioning.partitioningColumns().get(oldColumnIndex));
            if (newColumnIndex != null) {
                // Change the index of the top level column to the location in the new partitioning columns
                newPartitionFunctions.add(function.withTopLevelColumnIndex(newColumnIndex));
                // Some partition functions may be dropped so update the struct fields used in split partitioning must be updated
                newPartitionStructFields.add(tablePartitioning.partitionStructFields().get(functionIndex));
            }
        }

        IcebergPartitioningHandle newPartitioningHandle = new IcebergPartitioningHandle(false, newPartitionFunctions.build());
        if (partitioningHandle.isPresent() && !partitioningHandle.get().equals(newPartitioningHandle)) {
            // todo if bucketing is a power of two, we can adapt the bucketing
            return Optional.empty();
        }
        if (newPartitioningHandle.partitionFunctions().stream().map(IcebergPartitionFunction::transform).noneMatch(BUCKET::equals)) {
            // The table is only using value-based partitioning, and this can hurt performance if there is a filter
            // on the partitioning columns. This is something we may be able to support with statistics in the future.
            return Optional.empty();
        }

        return Optional.of(icebergTableHandle.withTablePartitioning(Optional.of(new IcebergTablePartitioning(
                true,
                newPartitioningHandle,
                partitioningColumns.stream().map(IcebergColumnHandle.class::cast).collect(toImmutableList()),
                newPartitionStructFields.build()))));
    }

    @Override
    public SchemaTableName getTableName(ConnectorSession session, ConnectorTableHandle table)
    {
        if (table instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            return corruptedTableHandle.schemaTableName();
        }
        return ((IcebergTableHandle) table).getSchemaTableName();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        IcebergTableHandle tableHandle = checkValidTableHandle(table);
        // This method does not calculate column metadata for the projected columns
        checkArgument(tableHandle.getProjectedColumns().isEmpty(), "Unexpected projected columns");
        BaseTable icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        List<ColumnMetadata> columns = getColumnMetadatas(SchemaParser.fromJson(tableHandle.getTableSchemaJson()), typeManager);
        return new ConnectorTableMetadata(tableHandle.getSchemaTableName(), columns, getIcebergTableProperties(icebergTable), getTableComment(icebergTable));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, RelationType> getRelationTypes(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableMap.Builder<SchemaTableName, RelationType> result = ImmutableMap.builder();
        for (TableInfo info : catalog.listTables(session, schemaName)) {
            result.put(info.tableName(), info.extendedRelationType().toRelationType());
        }
        return result.buildKeepingLast();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (IcebergColumnHandle columnHandle : getTopLevelColumns(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager)) {
            columnHandles.put(columnHandle.getName(), columnHandle);
        }
        columnHandles.put(PARTITION.getColumnName(), partitionColumnHandle());
        columnHandles.put(FILE_PATH.getColumnName(), pathColumnHandle());
        columnHandles.put(FILE_MODIFIED_TIME.getColumnName(), fileModifiedTimeColumnHandle());
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        return ColumnMetadata.builder()
                .setName(column.getName())
                .setType(column.getType())
                .setNullable(column.isNullable())
                .setComment(column.getComment())
                .setHidden(isMetadataColumnId(column.getId()))
                .build();
    }

    @Override
    public void validateScan(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        if (isQueryPartitionFilterRequiredForTable(session, table) && table.getEnforcedPredicate().isAll() && !table.getForAnalyze().orElseThrow()) {
            Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
            Optional<PartitionSpec> partitionSpec = table.getPartitionSpecJson()
                    .map(partitionSpecJson -> PartitionSpecParser.fromJson(schema, partitionSpecJson));
            if (partitionSpec.isEmpty() || partitionSpec.get().isUnpartitioned()) {
                return;
            }
            Set<Integer> columnsWithPredicates = new HashSet<>();
            table.getConstraintColumns().stream()
                    .map(IcebergColumnHandle::getId)
                    .forEach(columnsWithPredicates::add);
            table.getUnenforcedPredicate().getDomains().ifPresent(domain -> domain.keySet().stream()
                    .map(IcebergColumnHandle::getId)
                    .forEach(columnsWithPredicates::add));
            Set<Integer> partitionColumns = partitionSpec.get().fields().stream()
                    .filter(field -> !field.transform().isVoid())
                    .map(PartitionField::sourceId)
                    .collect(toImmutableSet());
            if (Collections.disjoint(columnsWithPredicates, partitionColumns)) {
                String partitionColumnNames = partitionSpec.get().fields().stream()
                        .filter(field -> !field.transform().isVoid())
                        .map(PartitionField::sourceId)
                        .map(id -> schema.idToName().get(id))
                        .collect(joining(", "));
                throw new TrinoException(
                        QUERY_REJECTED,
                        format("Filter required for %s on at least one of the partition columns: %s", table.getSchemaTableName(), partitionColumnNames));
            }
        }
    }

    private static boolean isQueryPartitionFilterRequiredForTable(ConnectorSession session, IcebergTableHandle table)
    {
        Set<String> requiredSchemas = getQueryPartitionFilterRequiredSchemas(session);
        // If query_partition_filter_required_schemas is empty then we would apply partition filter for all tables.
        return isQueryPartitionFilterRequired(session) &&
                (requiredSchemas.isEmpty() || requiredSchemas.contains(table.getSchemaName()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        List<SchemaTableName> schemaTableNames;
        if (prefix.getTable().isEmpty()) {
            schemaTableNames = catalog.listTables(session, prefix.getSchema()).stream()
                    .map(TableInfo::tableName)
                    .collect(toImmutableList());
        }
        else {
            schemaTableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        return Lists.partition(schemaTableNames, GET_METADATA_BATCH_SIZE).stream()
                .map(tableBatch -> {
                    ImmutableList.Builder<TableColumnsMetadata> tableMetadatas = ImmutableList.builderWithExpectedSize(tableBatch.size());
                    Set<SchemaTableName> remainingTables = new HashSet<>(tableBatch.size());
                    for (SchemaTableName tableName : tableBatch) {
                        if (redirectTable(session, tableName).isPresent()) {
                            tableMetadatas.add(TableColumnsMetadata.forRedirectedTable(tableName));
                        }
                        else {
                            remainingTables.add(tableName);
                        }
                    }

                    Map<SchemaTableName, List<ColumnMetadata>> loaded = catalog.tryGetColumnMetadata(session, ImmutableList.copyOf(remainingTables));
                    loaded.forEach((tableName, columns) -> {
                        remainingTables.remove(tableName);
                        tableMetadatas.add(TableColumnsMetadata.forTable(tableName, columns));
                    });

                    List<Callable<Optional<TableColumnsMetadata>>> tasks = remainingTables.stream()
                            .map(tableName -> (Callable<Optional<TableColumnsMetadata>>) () -> {
                                try {
                                    Table icebergTable = catalog.loadTable(session, tableName);
                                    List<ColumnMetadata> columns = getColumnMetadatas(icebergTable.schema(), typeManager);
                                    return Optional.of(TableColumnsMetadata.forTable(tableName, columns));
                                }
                                catch (TableNotFoundException e) {
                                    // Table disappeared during listing operation
                                    return Optional.empty();
                                }
                                catch (UnknownTableTypeException e) {
                                    // Skip unsupported table type in case that the table redirects are not enabled
                                    return Optional.empty();
                                }
                                catch (RuntimeException e) {
                                    // Table can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                                    log.warn(e, "Failed to access metadata of table %s during streaming table columns for %s", tableName, prefix);
                                    return Optional.empty();
                                }
                            })
                            .collect(toImmutableList());

                    try {
                        List<TableColumnsMetadata> taskResults = processWithAdditionalThreads(tasks, metadataFetchingExecutor).stream()
                                .flatMap(Optional::stream) // Flatten the Optionals into a stream
                                .collect(toImmutableList());

                        tableMetadatas.addAll(taskResults);
                    }
                    catch (ExecutionException e) {
                        throw new RuntimeException(e.getCause());
                    }

                    return tableMetadatas.build();
                })
                .flatMap(List::stream)
                .iterator();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationColumns(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationColumns
                    return ConnectorMetadata.super.streamRelationColumns(session, schemaName, relationFilter);
                });
    }

    @Override
    public Iterator<RelationCommentMetadata> streamRelationComments(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        return catalog.streamRelationComments(session, schemaName, relationFilter, tableName -> redirectTable(session, tableName).isPresent())
                .orElseGet(() -> {
                    // Catalog does not support streamRelationComments
                    return ConnectorMetadata.super.streamRelationComments(session, schemaName, relationFilter);
                });
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        catalog.createNamespace(session, schemaName, properties, owner);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (cascade) {
            List<String> nestedNamespaces = getChildNamespaces(session, schemaName);
            if (!nestedNamespaces.isEmpty()) {
                throw new TrinoException(
                        ICEBERG_CATALOG_ERROR,
                        format("Cannot drop non-empty schema: %s, contains %s nested schema(s)", schemaName, Joiner.on(", ").join(nestedNamespaces)));
            }

            for (SchemaTableName materializedView : listMaterializedViews(session, Optional.of(schemaName))) {
                dropMaterializedView(session, materializedView);
            }
            for (SchemaTableName viewName : listViews(session, Optional.of(schemaName))) {
                dropView(session, viewName);
            }
            for (SchemaTableName tableName : listTables(session, Optional.of(schemaName))) {
                ConnectorTableHandle tableHandle = getTableHandle(session, tableName, Optional.empty(), Optional.empty());
                if (tableHandle != null) {
                    // getTableHandle method returns null if the table is dropped concurrently
                    dropTable(session, tableHandle);
                }
            }
        }
        catalog.dropNamespace(session, schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        catalog.renameNamespace(session, source, target);
    }

    @Override
    public void setSchemaAuthorization(ConnectorSession session, String schemaName, TrinoPrincipal principal)
    {
        catalog.setNamespacePrincipal(session, schemaName, principal);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, SaveMode saveMode)
    {
        Optional<ConnectorTableLayout> layout = getNewTableLayout(session, tableMetadata);
        finishCreateTable(session, beginCreateTable(session, tableMetadata, layout, NO_RETRIES, saveMode == SaveMode.REPLACE), ImmutableList.of(), ImmutableList.of());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        catalog.updateTableComment(session, handle.getSchemaTableName(), comment);
    }

    @Override
    public void setViewComment(ConnectorSession session, SchemaTableName viewName, Optional<String> comment)
    {
        catalog.updateViewComment(session, viewName, comment);
    }

    @Override
    public void setViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public void setMaterializedViewColumnComment(ConnectorSession session, SchemaTableName viewName, String columnName, Optional<String> comment)
    {
        catalog.updateMaterializedViewColumnComment(session, viewName, columnName, comment);
    }

    @Override
    public Optional<ConnectorTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Schema schema = schemaFromMetadata(tableMetadata.getColumns());
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitioning(tableMetadata.getProperties()));
        return getWriteLayout(schema, partitionSpec, false);
    }

    @Override
    public Optional<io.trino.spi.type.Type> getSupportedType(ConnectorSession session, Map<String, Object> tableProperties, io.trino.spi.type.Type type)
    {
        io.trino.spi.type.Type newType = coerceType(type);
        if (type.getTypeSignature().equals(newType.getTypeSignature())) {
            return Optional.empty();
        }
        return Optional.of(newType);
    }

    private io.trino.spi.type.Type coerceType(io.trino.spi.type.Type type)
    {
        if (type == TINYINT || type == SMALLINT) {
            return INTEGER;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            return TIMESTAMP_TZ_MICROS;
        }
        if (type instanceof TimestampType) {
            return TIMESTAMP_MICROS;
        }
        if (type instanceof TimeType) {
            return TIME_MICROS;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(coerceType(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(coerceType(mapType.getKeyType()), coerceType(mapType.getValueType()), typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.from(rowType.getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), coerceType(field.getType())))
                    .collect(toImmutableList()));
        }
        return type;
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorTableLayout> layout, RetryMode retryMode, boolean replace)
    {
        verify(transaction == null, "transaction already set");
        String schemaName = tableMetadata.getTable().getSchemaName();
        if (!schemaExists(session, schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }

        String tableLocation = null;
        if (replace) {
            ConnectorTableHandle tableHandle = getTableHandle(session, tableMetadata.getTableSchema().getTable(), Optional.empty(), Optional.empty());
            if (tableHandle != null) {
                checkValidTableHandle(tableHandle);
                IcebergTableHandle table = (IcebergTableHandle) tableHandle;
                verifyTableVersionForUpdate(table);
                Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
                Optional<String> providedTableLocation = getTableLocation(tableMetadata.getProperties());
                if (providedTableLocation.isPresent() && !stripTrailingSlash(providedTableLocation.get()).equals(icebergTable.location())) {
                    throw new TrinoException(INVALID_TABLE_PROPERTY, format("The provided location '%s' does not match the existing table location '%s'", providedTableLocation.get(), icebergTable.location()));
                }
                validateNotModifyingOldSnapshot(table, icebergTable);
                tableLocation = icebergTable.location();
            }
        }

        if (tableLocation == null) {
            tableLocation = getTableLocation(tableMetadata.getProperties())
                    .orElseGet(() -> catalog.defaultTableLocation(session, tableMetadata.getTable()));
        }
        transaction = newCreateTableTransaction(catalog, tableMetadata, session, replace, tableLocation, allowedExtraProperties);
        Location location = Location.of(transaction.table().location());
        try {
            // S3 Tables internally assigns a unique location for each table
            if (!isS3Tables(location.toString())) {
                TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), transaction.table().io().properties());
                if (!replace && fileSystem.listFiles(location).hasNext()) {
                    throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, format("" +
                            "Cannot create a table on a non-empty location: %s, set 'iceberg.unique-table-location=true' in your Iceberg catalog properties " +
                            "to use unique table locations for every table.", location));
                }
            }
            return newWritableTableHandle(tableMetadata.getTable(), transaction.table());
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed checking new table's location: " + location, e);
        }
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergWritableTableHandle icebergTableHandle = (IcebergWritableTableHandle) tableHandle;
        try {
            if (fragments.isEmpty()) {
                // Commit the transaction if the table is being created without data
                AppendFiles appendFiles = transaction.newFastAppend();
                commitUpdateAndTransaction(appendFiles, session, transaction, "create table");
                transaction = null;
                return Optional.empty();
            }

            return finishInsert(session, icebergTableHandle, ImmutableList.of(), fragments, computedStatistics);
        }
        catch (AlreadyExistsException e) {
            // May happen when table has been already created concurrently.
            throw new TrinoException(TABLE_ALREADY_EXISTS, format("Table %s already exists", icebergTableHandle.name()), e);
        }
    }

    @Override
    public Optional<ConnectorTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(
                schema,
                table.getPartitionSpecJson().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle")));
        return getWriteLayout(schema, partitionSpec, false);
    }

    private Optional<ConnectorTableLayout> getWriteLayout(Schema tableSchema, PartitionSpec partitionSpec, boolean forceRepartitioning)
    {
        if (partitionSpec.isUnpartitioned()) {
            return Optional.empty();
        }

        StructType schemaAsStruct = tableSchema.asStruct();
        Map<Integer, NestedField> indexById = TypeUtil.indexById(schemaAsStruct);
        Map<Integer, Integer> indexParents = indexParents(schemaAsStruct);
        Map<Integer, List<Integer>> indexPaths = indexById.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(buildPath(indexParents, entry.getKey()))));

        List<IcebergColumnHandle> partitioningColumns = partitionSpec.fields().stream()
                .sorted(Comparator.comparingInt(PartitionField::sourceId))
                .map(field -> {
                    boolean isBaseColumn = !indexParents.containsKey(field.sourceId());
                    int sourceId;
                    if (isBaseColumn) {
                        sourceId = field.sourceId();
                    }
                    else {
                        sourceId = getRootFieldId(indexParents, field.sourceId());
                    }
                    Type sourceType = tableSchema.findType(sourceId);
                    // The source column, must be a primitive type and cannot be contained in a map or list, but may be nested in a struct.
                    // https://iceberg.apache.org/spec/#partitioning
                    if (sourceType.isMapType()) {
                        throw new TrinoException(NOT_SUPPORTED, "Partitioning field [" + field.name() + "] cannot be contained in a map");
                    }
                    if (sourceType.isListType()) {
                        throw new TrinoException(NOT_SUPPORTED, "Partitioning field [" + field.name() + "] cannot be contained in a array");
                    }
                    verify(indexById.containsKey(sourceId), "Cannot find source column for partition field %s", field);
                    return createColumnHandle(typeManager, sourceId, indexById, indexPaths);
                })
                .distinct()
                .collect(toImmutableList());
        List<String> partitioningColumnNames = partitioningColumns.stream()
                .map(column -> column.getName().toLowerCase(ENGLISH))
                .collect(toImmutableList());

        if (!forceRepartitioning && partitionSpec.fields().stream().allMatch(field -> field.transform().isIdentity())) {
            // Do not set partitioningHandle, to let engine determine whether to repartition data or not, on stat-based basis.
            return Optional.of(new ConnectorTableLayout(partitioningColumnNames));
        }
        IcebergPartitioningHandle partitioningHandle = IcebergPartitioningHandle.create(partitionSpec, typeManager, List.of());
        return Optional.of(new ConnectorTableLayout(partitioningHandle, partitioningColumnNames, true));
    }

    private static int getRootFieldId(Map<Integer, Integer> indexParents, int fieldId)
    {
        int rootFieldId = fieldId;
        while (indexParents.containsKey(rootFieldId)) {
            rootFieldId = indexParents.get(rootFieldId);
        }
        return rootFieldId;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);

        return newWritableTableHandle(table.getSchemaTableName(), icebergTable);
    }

    private List<String> getChildNamespaces(ConnectorSession session, String parentNamespace)
    {
        Optional<String> namespaceSeparator = catalog.getNamespaceSeparator();

        if (namespaceSeparator.isEmpty()) {
            return ImmutableList.of();
        }

        return catalog.listNamespaces(session).stream()
                .filter(namespace -> namespace.startsWith(parentNamespace + namespaceSeparator.get()))
                .collect(toImmutableList());
    }

    private IcebergWritableTableHandle newWritableTableHandle(SchemaTableName name, Table table)
    {
        return new IcebergWritableTableHandle(
                name,
                SchemaParser.toJson(table.schema()),
                transformValues(table.specs(), PartitionSpecParser::toJson),
                table.spec().specId(),
                getSupportedSortFields(table.schema(), table.sortOrder()),
                getPartitionColumns(table, typeManager),
                table.location(),
                getFileFormat(table),
                table.properties(),
                table.io().properties());
    }

    private static List<TrinoSortField> getSupportedSortFields(Schema schema, SortOrder sortOrder)
    {
        if (!sortOrder.isSorted()) {
            return ImmutableList.of();
        }
        Set<Integer> baseColumnFieldIds = schema.columns().stream()
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());

        ImmutableList.Builder<TrinoSortField> sortFields = ImmutableList.builder();
        for (SortField sortField : sortOrder.fields()) {
            if (!sortField.transform().isIdentity()) {
                continue;
            }
            if (!baseColumnFieldIds.contains(sortField.sourceId())) {
                continue;
            }

            sortFields.add(TrinoSortField.fromIceberg(sortField));
        }

        return sortFields.build();
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            transaction = null;
            return Optional.empty();
        }

        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;
        Table icebergTable = transaction.table();
        Schema schema = icebergTable.schema();
        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        schema.findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = isMergeManifestsOnWrite(session) ? transaction.newAppend() : transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(table.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics());
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        appendFiles.scanManifestsWith(icebergScanExecutor);
        commitUpdate(appendFiles, session, "insert");

        if (isS3Tables(icebergTable.location())) {
            log.debug("S3 Tables do not support statistics: %s", table.name());
        }
        else if (!computedStatistics.isEmpty()) {
            long newSnapshotId = icebergTable.currentSnapshot().snapshotId();

            CollectedStatistics collectedStatistics = processComputedTableStatistics(icebergTable, computedStatistics);
            StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                    session,
                    icebergTable,
                    newSnapshotId,
                    INCREMENTAL_UPDATE,
                    collectedStatistics);
            transaction.updateStatistics()
                    .setStatistics(statisticsFile)
                    .commit();
        }
        commitTransaction(transaction, "insert");
        transaction = null;

        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::path)
                .collect(toImmutableList())));
    }

    @Override
    public Optional<ConnectorTableExecuteHandle> getTableHandleForExecute(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            ConnectorTableHandle connectorTableHandle,
            String procedureName,
            Map<String, Object> executeProperties,
            RetryMode retryMode)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) connectorTableHandle;
        checkArgument(tableHandle.getTableType() == DATA, "Cannot execute table procedure %s on non-DATA table: %s", procedureName, tableHandle.getTableType());
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        if (tableHandle.getSnapshotId().isPresent() && (tableHandle.getSnapshotId().get() != icebergTable.currentSnapshot().snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot execute table procedure %s on old snapshot %s".formatted(procedureName, tableHandle.getSnapshotId().get()));
        }

        IcebergTableProcedureId procedureId;
        try {
            procedureId = IcebergTableProcedureId.valueOf(procedureName);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown procedure '" + procedureName + "'");
        }

        return switch (procedureId) {
            case OPTIMIZE -> getTableHandleForOptimize(tableHandle, icebergTable, executeProperties);
            case OPTIMIZE_MANIFESTS -> getTableHandleForOptimizeManifests(session, tableHandle);
            case DROP_EXTENDED_STATS -> getTableHandleForDropExtendedStats(session, tableHandle);
            case ROLLBACK_TO_SNAPSHOT -> getTableHandleForRollbackToSnapshot(session, tableHandle, executeProperties);
            case EXPIRE_SNAPSHOTS -> getTableHandleForExpireSnapshots(session, tableHandle, executeProperties);
            case REMOVE_ORPHAN_FILES -> getTableHandleForRemoveOrphanFiles(session, tableHandle, executeProperties);
            case ADD_FILES -> getTableHandleForAddFiles(session, accessControl, tableHandle, executeProperties);
            case ADD_FILES_FROM_TABLE -> getTableHandleForAddFilesFromTable(session, accessControl, tableHandle, executeProperties);
        };
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimize(
            IcebergTableHandle tableHandle,
            Table icebergTable,
            Map<String, Object> executeProperties)
    {
        DataSize maxScannedFileSize = (DataSize) executeProperties.get("file_size_threshold");

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE,
                new IcebergOptimizeHandle(
                        tableHandle.getSnapshotId(),
                        tableHandle.getTableSchemaJson(),
                        tableHandle.getPartitionSpecJson().orElseThrow(() -> new VerifyException("Partition spec missing in the table handle")),
                        getPartitionColumns(icebergTable, typeManager),
                        icebergTable.sortOrder().fields().stream()
                                .map(TrinoSortField::fromIceberg)
                                .collect(toImmutableList()),
                        getFileFormat(tableHandle.getStorageProperties()),
                        tableHandle.getStorageProperties(),
                        maxScannedFileSize),
                tableHandle.getTableLocation(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForOptimizeManifests(ConnectorSession session, IcebergTableHandle tableHandle)
    {
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                OPTIMIZE_MANIFESTS,
                new IcebergOptimizeManifestsHandle(),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForDropExtendedStats(ConnectorSession session, IcebergTableHandle tableHandle)
    {
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                DROP_EXTENDED_STATS,
                new IcebergDropExtendedStatsHandle(),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForExpireSnapshots(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
        int retainLast = firstNonNull(
                (Integer) executeProperties.get("retain_last"),
                propertyAsInt(icebergTable.properties(), MIN_SNAPSHOTS_TO_KEEP, MIN_SNAPSHOTS_TO_KEEP_DEFAULT));
        boolean cleanExpiredMetadata = (boolean) executeProperties.get("clean_expired_metadata");

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                EXPIRE_SNAPSHOTS,
                new IcebergExpireSnapshotsHandle(retentionThreshold, retainLast, cleanExpiredMetadata),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRemoveOrphanFiles(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        Duration retentionThreshold = (Duration) executeProperties.get(RETENTION_THRESHOLD);
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                REMOVE_ORPHAN_FILES,
                new IcebergRemoveOrphanFilesHandle(retentionThreshold),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForAddFiles(ConnectorSession session, ConnectorAccessControl accessControl, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        if (!addFilesProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "add_files procedure is disabled");
        }

        accessControl.checkCanInsertIntoTable(null, tableHandle.getSchemaTableName());

        String location = (String) requireProcedureArgument(executeProperties, "location");
        HiveStorageFormat format = (HiveStorageFormat) requireProcedureArgument(executeProperties, "format");
        RecursiveDirectory recursiveDirectory = (RecursiveDirectory) executeProperties.getOrDefault("recursive_directory", "fail");

        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ADD_FILES,
                new IcebergAddFilesHandle(location, format, recursiveDirectory),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForAddFilesFromTable(ConnectorSession session, ConnectorAccessControl accessControl, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        accessControl.checkCanInsertIntoTable(null, tableHandle.getSchemaTableName());

        String schemaName = (String) requireProcedureArgument(executeProperties, "schema_name");
        String tableName = (String) requireProcedureArgument(executeProperties, "table_name");
        @SuppressWarnings("unchecked")
        Map<String, String> partitionFilter = (Map<String, String>) executeProperties.get("partition_filter");
        RecursiveDirectory recursiveDirectory = (RecursiveDirectory) executeProperties.getOrDefault("recursive_directory", "fail");

        HiveMetastore metastore = metastoreFactory.orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "This catalog does not support add_files_from_table procedure"))
                .createMetastore(Optional.of(session.getIdentity()));
        SchemaTableName sourceName = new SchemaTableName(schemaName, tableName);
        io.trino.metastore.Table sourceTable = metastore.getTable(schemaName, tableName).orElseThrow(() -> new TableNotFoundException(sourceName));
        accessControl.checkCanSelectFromColumns(null, sourceName, Stream.concat(sourceTable.getDataColumns().stream(), sourceTable.getPartitionColumns().stream())
                .map(Column::getName)
                .collect(toImmutableSet()));

        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        checkProcedureArgument(
                icebergTable.schema().columns().size() >= sourceTable.getDataColumns().size(),
                "Target table should have at least %d columns but got %d", sourceTable.getDataColumns().size(), icebergTable.schema().columns().size());
        checkProcedureArgument(
                icebergTable.spec().fields().size() == sourceTable.getPartitionColumns().size(),
                "Numbers of partition columns should be equivalent. target: %d, source: %d", icebergTable.spec().fields().size(), sourceTable.getPartitionColumns().size());

        // TODO Add files from all partitions when partition filter is not provided
        checkProcedureArgument(
                sourceTable.getPartitionColumns().isEmpty() || partitionFilter != null,
                "partition_filter argument must be provided for partitioned tables");

        String transactionalProperty = sourceTable.getParameters().get(TRANSACTIONAL);
        if (parseBoolean(transactionalProperty)) {
            throw new TrinoException(NOT_SUPPORTED, "Adding files from transactional tables is unsupported");
        }
        if (!"MANAGED_TABLE".equalsIgnoreCase(sourceTable.getTableType()) && !"EXTERNAL_TABLE".equalsIgnoreCase(sourceTable.getTableType())) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support adding files from %s table type".formatted(sourceTable.getTableType()));
        }
        if (isSomeKindOfAView(sourceTable) || isIcebergTable(sourceTable) || isDeltaLakeTable(sourceTable) || isHudiTable(sourceTable)) {
            throw new TrinoException(NOT_SUPPORTED, "Adding files from non-Hive tables is unsupported");
        }
        if (sourceTable.getPartitionColumns().isEmpty() && partitionFilter != null && !partitionFilter.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "Partition filter is not supported for non-partitioned tables");
        }

        Set<String> missingDataColumns = new HashSet<>();
        Stream.of(sourceTable.getDataColumns(), sourceTable.getPartitionColumns())
                .flatMap(List::stream)
                .forEach(sourceColumn -> {
                    Types.NestedField targetColumn = icebergTable.schema().caseInsensitiveFindField(sourceColumn.getName());
                    if (targetColumn == null) {
                        if (sourceTable.getPartitionColumns().contains(sourceColumn)) {
                            throw new TrinoException(COLUMN_NOT_FOUND, "Partition column '%s' does not exist".formatted(sourceColumn.getName()));
                        }
                        missingDataColumns.add(sourceColumn.getName());
                        return;
                    }
                    ColumnIdentity columnIdentity = createColumnIdentity(targetColumn);
                    org.apache.iceberg.types.Type sourceColumnType = toIcebergType(typeManager.getType(getTypeSignature(sourceColumn.getType(), DEFAULT_PRECISION)), columnIdentity);
                    if (!targetColumn.type().equals(sourceColumnType)) {
                        throw new TrinoException(TYPE_MISMATCH, "Target '%s' column is '%s' type, but got source '%s' type".formatted(targetColumn.name(), targetColumn.type(), sourceColumnType));
                    }
                });
        if (missingDataColumns.size() == sourceTable.getDataColumns().size()) {
            throw new TrinoException(COLUMN_NOT_FOUND, "All columns in the source table do not exist in the target table");
        }

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ADD_FILES_FROM_TABLE,
                new IcebergAddFilesFromTableHandle(sourceTable, partitionFilter, recursiveDirectory),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private Optional<ConnectorTableExecuteHandle> getTableHandleForRollbackToSnapshot(ConnectorSession session, IcebergTableHandle tableHandle, Map<String, Object> executeProperties)
    {
        long snapshotId = (long) executeProperties.get("snapshot_id");
        Table icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());

        return Optional.of(new IcebergTableExecuteHandle(
                tableHandle.getSchemaTableName(),
                ROLLBACK_TO_SNAPSHOT,
                new IcebergRollbackToSnapshotHandle(snapshotId),
                icebergTable.location(),
                icebergTable.io().properties()));
    }

    private static Object requireProcedureArgument(Map<String, Object> properties, String name)
    {
        Object value = properties.get(name);
        checkProcedureArgument(value != null, "Required procedure argument '%s' is missing", name);
        return value;
    }

    @Override
    public Optional<ConnectorTableLayout> getLayoutForTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                return getLayoutForOptimize(session, executeHandle);
            case OPTIMIZE_MANIFESTS:
            case DROP_EXTENDED_STATS:
            case ROLLBACK_TO_SNAPSHOT:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private Optional<ConnectorTableLayout> getLayoutForOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
        // from performance perspective it is better to have lower number of bigger files than other way around
        // thus we force repartitioning for optimize to achieve this
        return getWriteLayout(icebergTable.schema(), icebergTable.spec(), true);
    }

    @Override
    public BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginTableExecute(
            ConnectorSession session,
            ConnectorTableExecuteHandle tableExecuteHandle,
            ConnectorTableHandle updatedSourceTableHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        IcebergTableHandle table = (IcebergTableHandle) updatedSourceTableHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                return beginOptimize(session, executeHandle, table);
            case OPTIMIZE_MANIFESTS:
            case DROP_EXTENDED_STATS:
            case ROLLBACK_TO_SNAPSHOT:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private BeginTableExecuteResult<ConnectorTableExecuteHandle, ConnectorTableHandle> beginOptimize(
            ConnectorSession session,
            IcebergTableExecuteHandle executeHandle,
            IcebergTableHandle table)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
        BaseTable icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        validateNotModifyingOldSnapshot(table, icebergTable);

        int tableFormatVersion = formatVersion(icebergTable);
        if (tableFormatVersion > OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION) {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "%s is not supported for Iceberg table format version > %d. Table %s format version is %s.",
                    OPTIMIZE.name(),
                    OPTIMIZE_MAX_SUPPORTED_TABLE_VERSION,
                    table.getSchemaTableName(),
                    tableFormatVersion));
        }

        beginTransaction(icebergTable);

        return new BeginTableExecuteResult<>(
                executeHandle,
                table.forOptimize(true, optimizeHandle.maxScannedFileSize()));
    }

    @Override
    public void finishTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE:
                finishOptimize(session, executeHandle, fragments, splitSourceInfo);
                return;
            case OPTIMIZE_MANIFESTS:
            case DROP_EXTENDED_STATS:
            case ROLLBACK_TO_SNAPSHOT:
            case EXPIRE_SNAPSHOTS:
            case REMOVE_ORPHAN_FILES:
            case ADD_FILES:
            case ADD_FILES_FROM_TABLE:
                // handled via executeTableExecute
        }
        throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
    }

    private void finishOptimize(ConnectorSession session, IcebergTableExecuteHandle executeHandle, Collection<Slice> fragments, List<Object> splitSourceInfo)
    {
        IcebergOptimizeHandle optimizeHandle = (IcebergOptimizeHandle) executeHandle.procedureHandle();
        Table icebergTable = transaction.table();

        // files to be deleted
        ImmutableSet.Builder<DataFile> scannedDataFilesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<DeleteFile> scannedDeleteFilesBuilder = ImmutableSet.builder();
        splitSourceInfo.stream().map(DataFileWithDeleteFiles.class::cast).forEach(dataFileWithDeleteFiles -> {
            scannedDataFilesBuilder.add(dataFileWithDeleteFiles.dataFile());
            scannedDeleteFilesBuilder.addAll(dataFileWithDeleteFiles.deleteFiles());
        });

        Set<DataFile> scannedDataFiles = scannedDataFilesBuilder.build();
        Set<DeleteFile> fullyAppliedDeleteFiles = scannedDeleteFilesBuilder.build();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        Set<DataFile> newFiles = new HashSet<>();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(optimizeHandle.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics());
            task.fileSplitOffsets().ifPresent(builder::withSplitOffsets);

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            newFiles.add(builder.build());
        }

        if (optimizeHandle.snapshotId().isEmpty() || scannedDataFiles.isEmpty() && fullyAppliedDeleteFiles.isEmpty() && newFiles.isEmpty()) {
            // Either the table is empty, or the table scan turned out to be empty, nothing to commit
            transaction = null;
            return;
        }

        RewriteFiles rewriteFiles = transaction.newRewrite();
        scannedDataFiles.forEach(rewriteFiles::deleteFile);
        fullyAppliedDeleteFiles.forEach(rewriteFiles::deleteFile);
        newFiles.forEach(rewriteFiles::addFile);

        // Table.snapshot method returns null if there is no matching snapshot
        Snapshot snapshot = requireNonNull(icebergTable.snapshot(optimizeHandle.snapshotId().get()), "snapshot is null");
        // Set dataSequenceNumber to avoid contention between OPTIMIZE and concurrent writing of equality deletes
        rewriteFiles.dataSequenceNumber(snapshot.sequenceNumber());
        rewriteFiles.validateFromSnapshot(snapshot.snapshotId());
        rewriteFiles.scanManifestsWith(icebergScanExecutor);
        commitUpdate(rewriteFiles, session, "optimize");

        long newSnapshotId = icebergTable.currentSnapshot().snapshotId();
        StatisticsFile newStatsFile = tableStatisticsWriter.rewriteStatisticsFile(session, icebergTable, newSnapshotId);
        transaction.updateStatistics()
                .setStatistics(newStatsFile)
                .commit();

        commitTransaction(transaction, "optimize");
        transaction = null;
    }

    private static void commitUpdateAndTransaction(SnapshotUpdate<?> update, ConnectorSession session, Transaction transaction, String operation)
    {
        commitUpdate(update, session, operation);
        commitTransaction(transaction, operation);
    }

    private static void commitUpdate(SnapshotUpdate<?> update, ConnectorSession session, String operation)
    {
        try {
            commit(update, session);
        }
        catch (UncheckedIOException | ValidationException | CommitFailedException | CommitStateUnknownException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Failed to commit during %s: %s", operation, firstNonNull(e.getMessage(), e)), e);
        }
    }

    private static void commitTransaction(Transaction transaction, String operation)
    {
        try {
            transaction.commitTransaction();
        }
        catch (UncheckedIOException | ValidationException | CommitFailedException | CommitStateUnknownException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, format("Failed to commit the transaction during %s: %s", operation, firstNonNull(e.getMessage(), e)), e);
        }
    }

    @Override
    public Map<String, Long> executeTableExecute(ConnectorSession session, ConnectorTableExecuteHandle tableExecuteHandle)
    {
        IcebergTableExecuteHandle executeHandle = (IcebergTableExecuteHandle) tableExecuteHandle;
        switch (executeHandle.procedureId()) {
            case OPTIMIZE_MANIFESTS:
                executeOptimizeManifests(session, executeHandle);
                return ImmutableMap.of();
            case DROP_EXTENDED_STATS:
                executeDropExtendedStats(session, executeHandle);
                return ImmutableMap.of();
            case ROLLBACK_TO_SNAPSHOT:
                executeRollbackToSnapshot(session, executeHandle);
                return ImmutableMap.of();
            case EXPIRE_SNAPSHOTS:
                executeExpireSnapshots(session, executeHandle);
                return ImmutableMap.of();
            case REMOVE_ORPHAN_FILES:
                return executeRemoveOrphanFiles(session, executeHandle);
            case ADD_FILES:
                executeAddFiles(session, executeHandle);
                return ImmutableMap.of();
            case ADD_FILES_FROM_TABLE:
                executeAddFilesFromTable(session, executeHandle);
                return ImmutableMap.of();
            default:
                throw new IllegalArgumentException("Unknown procedure '" + executeHandle.procedureId() + "'");
        }
    }

    private void executeOptimizeManifests(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergOptimizeManifestsHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());

        BaseTable icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
        List<ManifestFile> manifests = loadAllManifestsFromSnapshot(icebergTable, icebergTable.currentSnapshot());
        if (manifests.isEmpty()) {
            return;
        }
        long manifestTargetSizeBytes = icebergTable.operations().current().propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        if (manifests.size() == 1 && manifests.getFirst().length() < manifestTargetSizeBytes) {
            return;
        }
        long totalManifestsSize = manifests.stream().mapToLong(ManifestFile::length).sum();
        // Having too many open manifest writers can potentially cause OOM on the coordinator
        long targetManifestClusters = Math.min(((totalManifestsSize + manifestTargetSizeBytes - 1) / manifestTargetSizeBytes), 100);

        beginTransaction(icebergTable);
        RewriteManifests rewriteManifests = transaction.rewriteManifests();
        Types.StructType structType = icebergTable.spec().partitionType();
        rewriteManifests
                .clusterBy(file -> {
                    // Cluster by partitions for better locality when reading data files
                    StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(structType).set(file.partition());
                    // Limit the number of clustering buckets to avoid creating too many small manifest files
                    return Objects.hash(partitionWrapper) % targetManifestClusters;
                })
                .scanManifestsWith(icebergScanExecutor)
                .commit();
        commitTransaction(transaction, "optimize manifests");
        transaction = null;
    }

    private void executeDropExtendedStats(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergDropExtendedStatsHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());

        try {
            Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
            beginTransaction(icebergTable);
            UpdateStatistics updateStatistics = transaction.updateStatistics();
            for (StatisticsFile statisticsFile : icebergTable.statisticsFiles()) {
                updateStatistics.removeStatistics(statisticsFile.snapshotId());
            }
            updateStatistics.commit();
            commitTransaction(transaction, "drop extended stats");
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, e);
        }

        transaction = null;
    }

    private void executeRollbackToSnapshot(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        checkArgument(executeHandle.procedureHandle() instanceof IcebergRollbackToSnapshotHandle, "Unexpected procedure handle %s", executeHandle.procedureHandle());
        long snapshotId = ((IcebergRollbackToSnapshotHandle) executeHandle.procedureHandle()).snapshotId();

        try {
            Table icebergTable = catalog.loadTable(session, executeHandle.schemaTableName());
            icebergTable.manageSnapshots().setCurrentSnapshot(snapshotId).commit();
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, e);
        }
    }

    private void executeExpireSnapshots(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergExpireSnapshotsHandle expireSnapshotsHandle = (IcebergExpireSnapshotsHandle) executeHandle.procedureHandle();

        BaseTable table = catalog.loadTable(session, executeHandle.schemaTableName());
        Duration retention = requireNonNull(expireSnapshotsHandle.retentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.schemaTableName(),
                EXPIRE_SNAPSHOTS.name(),
                retention,
                getExpireSnapshotMinRetention(session),
                IcebergConfig.EXPIRE_SNAPSHOTS_MIN_RETENTION,
                IcebergSessionProperties.EXPIRE_SNAPSHOTS_MIN_RETENTION);

        // ForwardingFileIo handles bulk operations so no separate function implementation is needed
        try {
            table.expireSnapshots()
                    .expireOlderThan(session.getStart().toEpochMilli() - retention.toMillis())
                    .retainLast(expireSnapshotsHandle.retainLast())
                    .cleanExpiredMetadata(expireSnapshotsHandle.cleanExpiredMetadata())
                    .planWith(icebergScanExecutor)
                    .commit();
        }
        catch (NotFoundException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, e);
        }
    }

    private static void validateTableExecuteParameters(
            BaseTable table,
            SchemaTableName schemaTableName,
            String procedureName,
            Duration retentionThreshold,
            Duration minRetention,
            String minRetentionParameterName,
            String sessionMinRetentionParameterName)
    {
        int tableFormatVersion = formatVersion(table);
        if (tableFormatVersion > CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION) {
            // It is not known if future version won't bring any new kind of metadata or data files
            // because of the way procedures are implemented it is safer to fail here than to potentially remove
            // files that should stay there
            throw new TrinoException(NOT_SUPPORTED, format("%s is not supported for Iceberg table format version > %d. " +
                            "Table %s format version is %s.",
                    procedureName,
                    CLEANING_UP_PROCEDURES_MAX_SUPPORTED_TABLE_VERSION,
                    schemaTableName,
                    tableFormatVersion));
        }
        Map<String, String> properties = table.properties();
        if (properties.containsKey(WRITE_LOCATION_PROVIDER_IMPL)) {
            throw new TrinoException(NOT_SUPPORTED, "Table " + schemaTableName + " specifies " + properties.get(WRITE_LOCATION_PROVIDER_IMPL) +
                    " as a location provider. Writing to Iceberg tables with custom location provider is not supported.");
        }

        Duration retention = requireNonNull(retentionThreshold, "retention is null");
        checkProcedureArgument(retention.compareTo(minRetention) >= 0,
                "Retention specified (%s) is shorter than the minimum retention configured in the system (%s). " +
                        "Minimum retention can be changed with %s configuration property or iceberg.%s session property",
                retention,
                minRetention,
                minRetentionParameterName,
                sessionMinRetentionParameterName);
    }

    public Map<String, Long> executeRemoveOrphanFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergRemoveOrphanFilesHandle removeOrphanFilesHandle = (IcebergRemoveOrphanFilesHandle) executeHandle.procedureHandle();

        BaseTable table = catalog.loadTable(session, executeHandle.schemaTableName());
        Duration retention = requireNonNull(removeOrphanFilesHandle.retentionThreshold(), "retention is null");
        validateTableExecuteParameters(
                table,
                executeHandle.schemaTableName(),
                REMOVE_ORPHAN_FILES.name(),
                retention,
                getRemoveOrphanFilesMinRetention(session),
                IcebergConfig.REMOVE_ORPHAN_FILES_MIN_RETENTION,
                IcebergSessionProperties.REMOVE_ORPHAN_FILES_MIN_RETENTION);

        if (table.currentSnapshot() == null) {
            log.debug("Skipping remove_orphan_files procedure for empty table %s", table);
            return ImmutableMap.of();
        }

        Instant expiration = session.getStart().minusMillis(retention.toMillis());
        return removeOrphanFiles(table, session, executeHandle.schemaTableName(), expiration, executeHandle.fileIoProperties());
    }

    private Map<String, Long> removeOrphanFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration, Map<String, String> fileIoProperties)
    {
        Set<String> processedManifestFilePaths = new HashSet<>();
        // Similarly to issues like https://github.com/trinodb/trino/issues/13759, equivalent paths may have different String
        // representations due to things like double slashes. Using file names may result in retaining files which could be removed.
        // However, in practice Iceberg metadata and data files have UUIDs in their names which makes this unlikely.
        Set<String> validFileNames = Sets.newConcurrentHashSet();
        List<Future<?>> manifestScanFutures = new ArrayList<>();

        for (Snapshot snapshot : table.snapshots()) {
            String manifestListLocation = snapshot.manifestListLocation();
            List<ManifestFile> allManifests;
            if (manifestListLocation != null) {
                validFileNames.add(fileName(manifestListLocation));
                allManifests = loadAllManifestsFromManifestList(table, manifestListLocation);
            }
            else {
                // This is to maintain support for V1 tables which have embedded manifest lists
                allManifests = loadAllManifestsFromSnapshot(table, snapshot);
            }

            for (ManifestFile manifest : allManifests) {
                if (!processedManifestFilePaths.add(manifest.path())) {
                    // Already read this manifest
                    continue;
                }

                validFileNames.add(fileName(manifest.path()));
                manifestScanFutures.add(icebergScanExecutor.submit(() -> {
                    try (ManifestReader<? extends ContentFile<?>> manifestReader = readerForManifest(manifest, table)) {
                        for (ContentFile<?> contentFile : manifestReader.select(ImmutableList.of("file_path"))) {
                            validFileNames.add(fileName(contentFile.location()));
                        }
                    }
                    catch (IOException | UncheckedIOException e) {
                        throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Unable to list manifest file content from " + manifest.path(), e);
                    }
                    catch (NotFoundException e) {
                        throw new TrinoException(ICEBERG_INVALID_METADATA, "Manifest file does not exist: " + manifest.path());
                    }
                }));
            }
        }

        metadataFileLocations(table, false).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        statisticsFilesLocations(table).stream()
                .map(IcebergUtil::fileName)
                .forEach(validFileNames::add);

        validFileNames.add("version-hint.text");

        try {
            manifestScanFutures.forEach(MoreFutures::getFutureValue);
            // All futures completed normally
            manifestScanFutures.clear();
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            manifestScanFutures.forEach(future -> future.cancel(true));
        }
        ScanAndDeleteResult result = scanAndDeleteInvalidFiles(table, session, schemaTableName, expiration, validFileNames, fileIoProperties);
        log.info("remove_orphan_files for table %s processed %d manifest files, found %d active files, scanned %d files, deleted %d files",
                schemaTableName,
                processedManifestFilePaths.size(),
                validFileNames.size() - 1, // excluding version-hint.text
                result.scannedFilesCount(),
                result.deletedFilesCount());
        return ImmutableMap.of(
                "processed_manifests_count", (long) processedManifestFilePaths.size(),
                "active_files_count", (long) validFileNames.size() - 1, // excluding version-hint.text
                "scanned_files_count", result.scannedFilesCount(),
                "deleted_files_count", result.deletedFilesCount());
    }

    public void executeAddFiles(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergAddFilesHandle addFilesHandle = (IcebergAddFilesHandle) executeHandle.procedureHandle();
        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        addFiles(
                session,
                fileSystem,
                catalog,
                executeHandle.schemaTableName(),
                addFilesHandle.location(),
                addFilesHandle.format(),
                addFilesHandle.recursiveDirectory(),
                icebergScanExecutor);
    }

    public void executeAddFilesFromTable(ConnectorSession session, IcebergTableExecuteHandle executeHandle)
    {
        IcebergAddFilesFromTableHandle addFilesHandle = (IcebergAddFilesFromTableHandle) executeHandle.procedureHandle();
        Table table = catalog.loadTable(session, executeHandle.schemaTableName());
        TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), table.io().properties());
        addFilesFromTable(
                session,
                fileSystem,
                metastoreFactory.orElseThrow(),
                table,
                addFilesHandle.table(),
                addFilesHandle.partitionFilter(),
                addFilesHandle.recursiveDirectory(),
                icebergScanExecutor);
    }

    private ScanAndDeleteResult scanAndDeleteInvalidFiles(Table table, ConnectorSession session, SchemaTableName schemaTableName, Instant expiration, Set<String> validFiles, Map<String, String> fileIoProperties)
    {
        List<Future<?>> deleteFutures = new ArrayList<>();
        long scannedFilesCount = 0;
        long deletedFilesCount = 0;
        try {
            List<Location> filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), fileIoProperties);
            FileIterator allFiles = fileSystem.listFiles(Location.of(table.location()));
            while (allFiles.hasNext()) {
                FileEntry entry = allFiles.next();
                scannedFilesCount++;
                if (entry.lastModified().isBefore(expiration) && !validFiles.contains(entry.location().fileName())) {
                    filesToDelete.add(entry.location());
                    deletedFilesCount++;
                    if (filesToDelete.size() >= DELETE_BATCH_SIZE) {
                        List<Location> finalFilesToDelete = filesToDelete;
                        deleteFutures.add(icebergFileDeleteExecutor.submit(() -> deleteFiles(finalFilesToDelete, schemaTableName, fileSystem)));
                        filesToDelete = new ArrayList<>(DELETE_BATCH_SIZE);
                    }
                }
                else {
                    log.debug("%s file retained while removing orphan files %s", entry.location(), schemaTableName.getTableName());
                }
            }
            if (!filesToDelete.isEmpty()) {
                log.debug("Deleting files while removing orphan files for table %s %s", schemaTableName, filesToDelete);
                fileSystem.deleteFiles(filesToDelete);
            }

            deleteFutures.forEach(MoreFutures::getFutureValue);
            // All futures completed normally
            deleteFutures.clear();
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed removing orphan files for table: " + schemaTableName, e);
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            deleteFutures.forEach(future -> future.cancel(true));
        }
        return new ScanAndDeleteResult(scannedFilesCount, deletedFilesCount);
    }

    private record ScanAndDeleteResult(long scannedFilesCount, long deletedFilesCount) {}

    private void deleteFiles(List<Location> files, SchemaTableName schemaTableName, TrinoFileSystem fileSystem)
    {
        log.debug("Deleting files while removing orphan files for table %s [%s]", schemaTableName, files);
        try {
            fileSystem.deleteFiles(files);
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Failed removing orphan files for table: " + schemaTableName, e);
        }
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId, BoundSignature boundSignature)
    {
        return FunctionDependencyDeclaration.NO_DEPENDENCIES;
    }

    @Override
    public Optional<Object> getInfo(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        List<String> partitionFields = icebergTableHandle.getPartitionSpecJson()
                .map(partitionSpecJson -> PartitionSpecParser.fromJson(SchemaParser.fromJson(icebergTableHandle.getTableSchemaJson()), partitionSpecJson)
                        .fields().stream()
                        .map(field -> field.name() + ": " + field.transform())
                        .collect(toImmutableList()))
                .orElse(ImmutableList.of());

        Map<String, String> summary = ImmutableMap.of();
        if (icebergTableHandle.getSnapshotId().isPresent()) {
            Table table = catalog.loadTable(session, icebergTableHandle.getSchemaTableName());
            summary = table.snapshot(icebergTableHandle.getSnapshotId().get()).summary();
        }
        Optional<String> totalRecords = Optional.ofNullable(summary.get(TOTAL_RECORDS_PROP));
        Optional<String> deletedRecords = Optional.ofNullable(summary.get(DELETED_RECORDS_PROP));
        Optional<String> totalDataFiles = Optional.ofNullable(summary.get(TOTAL_DATA_FILES_PROP));
        Optional<String> totalDeleteFiles = Optional.ofNullable(summary.get(TOTAL_DELETE_FILES_PROP));

        return Optional.of(new IcebergInputInfo(
                icebergTableHandle.getFormatVersion(),
                icebergTableHandle.getSnapshotId(),
                partitionFields,
                getFileFormat(icebergTableHandle.getStorageProperties()).name(),
                totalRecords,
                deletedRecords,
                totalDataFiles,
                totalDeleteFiles));
    }

    @Override
    public io.trino.spi.metrics.Metrics getMetrics(ConnectorSession session)
    {
        return catalog.getMetrics();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (tableHandle instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            catalog.dropCorruptedTable(session, corruptedTableHandle.schemaTableName());
        }
        else {
            catalog.dropTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTable)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        catalog.renameTable(session, handle.getSchemaTableName(), newTable);
    }

    @Override
    public void setTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Optional<Object>> properties)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

        Set<String> unsupportedProperties = difference(properties.keySet(), UPDATABLE_TABLE_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new TrinoException(NOT_SUPPORTED, "The following properties cannot be updated: " + String.join(", ", unsupportedProperties));
        }

        beginTransaction(icebergTable);
        UpdateProperties updateProperties = transaction.updateProperties();

        if (properties.containsKey(EXTRA_PROPERTIES_PROPERTY)) {
            @SuppressWarnings("unchecked")
            Map<String, String> extraProperties = (Map<String, String>) properties.get(EXTRA_PROPERTIES_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The extra_properties property cannot be empty"));
            verifyExtraProperties(properties.keySet(), extraProperties, allowedExtraProperties);
            extraProperties.forEach(updateProperties::set);
        }

        if (properties.containsKey(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY)) {
            checkFormatForProperty(getFileFormat(icebergTable).toIceberg(), FileFormat.PARQUET, PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY);
            @SuppressWarnings("unchecked")
            List<String> parquetBloomFilterColumns = (List<String>) properties.get(PARQUET_BLOOM_FILTER_COLUMNS_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The parquet_bloom_filter_columns property cannot be empty"));
            validateParquetBloomFilterColumns(getColumnMetadatas(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager), parquetBloomFilterColumns);

            Set<String> existingParquetBloomFilterColumns = icebergTable.properties().keySet().stream()
                    .filter(key -> key.startsWith(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX))
                    .map(key -> key.substring(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX.length()))
                    .collect(toImmutableSet());
            Set<String> removeParquetBloomFilterColumns = Sets.difference(existingParquetBloomFilterColumns, Set.copyOf(parquetBloomFilterColumns));
            removeParquetBloomFilterColumns.forEach(column -> updateProperties.remove(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + column));
            parquetBloomFilterColumns.forEach(column -> updateProperties.set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + column, "true"));
        }

        if (properties.containsKey(ORC_BLOOM_FILTER_COLUMNS_PROPERTY)) {
            checkFormatForProperty(getFileFormat(icebergTable).toIceberg(), FileFormat.ORC, ORC_BLOOM_FILTER_COLUMNS_PROPERTY);
            @SuppressWarnings("unchecked")
            List<String> orcBloomFilterColumns = (List<String>) properties.get(ORC_BLOOM_FILTER_COLUMNS_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The orc_bloom_filter_columns property cannot be empty"));
            if (orcBloomFilterColumns.isEmpty()) {
                updateProperties.remove(ORC_BLOOM_FILTER_COLUMNS);
            }
            else {
                validateOrcBloomFilterColumns(getColumnMetadatas(SchemaParser.fromJson(table.getTableSchemaJson()), typeManager), orcBloomFilterColumns);
                updateProperties.set(ORC_BLOOM_FILTER_COLUMNS, Joiner.on(",").join(orcBloomFilterColumns));
            }
        }

        IcebergFileFormat oldFileFormat = getFileFormat(icebergTable.properties());
        IcebergFileFormat newFileFormat = oldFileFormat;

        if (properties.containsKey(FILE_FORMAT_PROPERTY)) {
            newFileFormat = (IcebergFileFormat) properties.get(FILE_FORMAT_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The format property cannot be empty"));
            updateProperties.defaultFormat(newFileFormat.toIceberg());
        }

        if (properties.containsKey(FORMAT_VERSION_PROPERTY)) {
            // UpdateProperties#commit will trigger any necessary metadata updates required for the new spec version
            int formatVersion = (int) properties.get(FORMAT_VERSION_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The format_version property cannot be empty"));
            updateProperties.set(FORMAT_VERSION, Integer.toString(formatVersion));
        }

        Map<String, String> propertiesForCompression = calculateTableCompressionProperties(oldFileFormat, newFileFormat, icebergTable.properties(), properties.entrySet().stream()
                .filter(e -> e.getValue().isPresent())
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().get())));

        propertiesForCompression.forEach(updateProperties::set);

        if (properties.containsKey(MAX_COMMIT_RETRY)) {
            int maxCommitRetry = (int) properties.get(MAX_COMMIT_RETRY)
                    .orElseThrow(() -> new IllegalArgumentException("The max_commit_retry property cannot be empty"));
            updateProperties.set(COMMIT_NUM_RETRIES, Integer.toString(maxCommitRetry));
        }

        if (properties.containsKey(DELETE_AFTER_COMMIT_ENABLED)) {
            boolean deleteAfterCommitEnabled = (boolean) properties.get(DELETE_AFTER_COMMIT_ENABLED)
                    .orElseThrow(() -> new IllegalArgumentException("The %s property cannot be empty".formatted(DELETE_AFTER_COMMIT_ENABLED)));
            updateProperties.set(METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.toString(deleteAfterCommitEnabled));
        }

        if (properties.containsKey(MAX_PREVIOUS_VERSIONS)) {
            int maxPreviousVersions = (int) properties.get(MAX_PREVIOUS_VERSIONS)
                    .orElseThrow(() -> new IllegalArgumentException("The %s property cannot be empty".formatted(MAX_PREVIOUS_VERSIONS)));
            updateProperties.set(METADATA_PREVIOUS_VERSIONS_MAX, Integer.toString(maxPreviousVersions));
        }

        if (properties.containsKey(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)) {
            boolean objectStoreEnabled = (boolean) properties.get(OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The object_store_enabled property cannot be empty"));
            updateProperties.set(OBJECT_STORE_ENABLED, Boolean.toString(objectStoreEnabled));
        }

        if (properties.containsKey(DATA_LOCATION_PROPERTY)) {
            String dataLocation = (String) properties.get(DATA_LOCATION_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The data_location property cannot be empty"));
            boolean objectStoreEnabled = (boolean) properties.getOrDefault(
                    OBJECT_STORE_LAYOUT_ENABLED_PROPERTY,
                    Optional.of(Boolean.parseBoolean(icebergTable.properties().get(OBJECT_STORE_ENABLED)))).orElseThrow();
            if (!objectStoreEnabled) {
                throw new TrinoException(INVALID_TABLE_PROPERTY, "Data location can only be set when object store layout is enabled");
            }
            updateProperties.set(WRITE_DATA_LOCATION, dataLocation);
        }

        if (properties.containsKey(IcebergTableProperties.WRITE_DELETE_MODE)) {
            UpdateMode writeDeleteMode = (UpdateMode) properties.get(IcebergTableProperties.WRITE_DELETE_MODE)
                    .orElseThrow(() -> new IllegalArgumentException("The write_delete_mode property cannot be empty"));
            updateProperties.set(DELETE_MODE, writeDeleteMode.getIcebergProperty());
        }

        if (properties.containsKey(IcebergTableProperties.WRITE_UPDATE_MODE)) {
            UpdateMode writeUpdateMode = (UpdateMode) properties.get(IcebergTableProperties.WRITE_UPDATE_MODE)
                    .orElseThrow(() -> new IllegalArgumentException("The write_update_mode property cannot be empty"));
            updateProperties.set(UPDATE_MODE, writeUpdateMode.getIcebergProperty());
        }

        if (properties.containsKey(IcebergTableProperties.WRITE_MERGE_MODE)) {
            UpdateMode writeMergeMode = (UpdateMode) properties.get(IcebergTableProperties.WRITE_MERGE_MODE)
                    .orElseThrow(() -> new IllegalArgumentException("The write_merge_mode property cannot be empty"));
            updateProperties.set(MERGE_MODE, writeMergeMode.getIcebergProperty());
        }

        try {
            updateProperties.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set new property values", e);
        }

        if (properties.containsKey(PARTITIONING_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> partitionColumns = (List<String>) properties.get(PARTITIONING_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The partitioning property cannot be empty"));
            updatePartitioning(icebergTable, transaction, partitionColumns);
        }

        if (properties.containsKey(SORTED_BY_PROPERTY)) {
            @SuppressWarnings("unchecked")
            List<String> sortColumns = (List<String>) properties.get(SORTED_BY_PROPERTY)
                    .orElseThrow(() -> new IllegalArgumentException("The sorted_by property cannot be empty"));
            ReplaceSortOrder replaceSortOrder = transaction.replaceSortOrder();
            parseSortFields(replaceSortOrder, sortColumns);
            try {
                replaceSortOrder.commit();
            }
            catch (RuntimeException e) {
                throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set the sorted_by property", e);
            }
        }

        commitTransaction(transaction, "set table properties");
    }

    public static Map<String, String> calculateTableCompressionProperties(IcebergFileFormat oldFileFormat, IcebergFileFormat newFileFormat, Map<String, String> existingProperties, Map<String, Object> inputProperties)
    {
        ImmutableMap.Builder<String, String> newCompressionProperties = ImmutableMap.builder();

        Optional<HiveCompressionCodec> oldCompressionCodec = getHiveCompressionCodec(oldFileFormat, existingProperties);
        Optional<HiveCompressionCodec> newCompressionCodec = IcebergTableProperties.getCompressionCodec(inputProperties);

        Optional<HiveCompressionCodec> compressionCodec = newCompressionCodec.or(() -> oldCompressionCodec);

        validateCompression(newFileFormat, compressionCodec);

        compressionCodec.ifPresent(hiveCompressionCodec -> newCompressionProperties.put(getCompressionPropertyName(newFileFormat), hiveCompressionCodec.name()));

        return newCompressionProperties.buildOrThrow();
    }

    private static void updatePartitioning(Table icebergTable, Transaction transaction, List<String> partitionColumns)
    {
        UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
        Set<PartitionField> existingPartitionFields = ImmutableSet.copyOf(icebergTable.spec().fields());
        Schema schema = icebergTable.schema();
        if (partitionColumns.isEmpty()) {
            existingPartitionFields.stream()
                    .map(partitionField -> toIcebergTerm(schema, partitionField))
                    .forEach(updatePartitionSpec::removeField);
        }
        else {
            PartitionSpec partitionSpec = parsePartitionFields(schema, partitionColumns, getAllPartitionFields(icebergTable));
            Set<PartitionField> partitionFields = ImmutableSet.copyOf(partitionSpec.fields());
            difference(existingPartitionFields, partitionFields).stream()
                    .map(PartitionField::name)
                    .forEach(updatePartitionSpec::removeField);
            difference(partitionFields, existingPartitionFields)
                    .forEach(partitionField -> updatePartitionSpec.addField(partitionField.name(), toIcebergTerm(schema, partitionField)));
        }

        try {
            updatePartitionSpec.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set new partitioning value", e);
        }
    }

    private static Term toIcebergTerm(Schema schema, PartitionField partitionField)
    {
        return Expressions.transform(schema.findColumnName(partitionField.sourceId()), partitionField.transform());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column, ColumnPosition position)
    {
        // Spark doesn't support adding a NOT NULL column to Iceberg tables
        // Also, Spark throws an exception when reading the table if we add such columns and execute a rollback procedure
        // because they keep returning the latest table definition even after the rollback https://github.com/apache/iceberg/issues/5591
        // Even when a table is empty, this connector doesn't support adding not null columns to avoid the above Spark failure
        if (!column.isNullable()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support adding not null columns");
        }
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        // Start explicitly with highestFieldId + 2 to account for existing columns and the new one being
        // added - instead of relying on addColumn in iceberg library to assign Ids
        AtomicInteger nextFieldId = new AtomicInteger(icebergTable.schema().highestFieldId() + 2);
        try {
            UpdateSchema updateSchema = icebergTable.updateSchema();
            updateSchema.addColumn(null, column.getName(), toIcebergTypeForNewColumn(column.getType(), nextFieldId), column.getComment());
            switch (position) {
                case ColumnPosition.First _ -> updateSchema.moveFirst(column.getName());
                case ColumnPosition.After after -> updateSchema.moveAfter(column.getName(), after.columnName());
                case ColumnPosition.Last _ -> {}
            }
            updateSchema.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void addField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> parentPath, String fieldName, io.trino.spi.type.Type type, boolean ignoreExisting)
    {
        // Iceberg disallows ambiguous field names in a table. e.g. (a row(b int), "a.b" int)
        String parentName = String.join(".", parentPath);

        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        NestedField parent = icebergTable.schema().caseInsensitiveFindField(parentName);

        String caseSensitiveParentName = icebergTable.schema().findColumnName(parent.fieldId());

        Types.StructType structType;
        if (parent.type().isListType()) {
            // list(struct...)
            structType = parent.type().asListType().elementType().asStructType();
        }
        else {
            // just struct
            structType = parent.type().asStructType();
        }

        NestedField field = structType.caseInsensitiveField(fieldName);
        if (field != null) {
            if (ignoreExisting) {
                return;
            }
            throw new TrinoException(COLUMN_ALREADY_EXISTS, "Field '%s' already exists".formatted(fieldName));
        }

        try {
            icebergTable.updateSchema()
                    .addColumn(caseSensitiveParentName, fieldName, toIcebergTypeForNewColumn(type, new AtomicInteger())) // Iceberg library assigns fresh id internally
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add field: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        dropField(session, tableHandle, handle.getName());
    }

    @Override
    public void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, List<String> fieldPath)
    {
        IcebergColumnHandle handle = (IcebergColumnHandle) column;
        // Iceberg disallows ambiguous field names in a table. e.g. (a row(b int), "a.b" int)
        String name = String.join(".", ImmutableList.<String>builder().add(handle.getName()).addAll(fieldPath).build());
        dropField(session, tableHandle, name);
    }

    private void dropField(ConnectorSession session, ConnectorTableHandle tableHandle, String name)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        long fieldId = icebergTable.schema().findField(name).fieldId();
        boolean isPartitionColumn = icebergTable.spec().fields().stream()
                .anyMatch(field -> field.sourceId() == fieldId);
        if (isPartitionColumn) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop partition field: " + name);
        }
        boolean isSortColumn = icebergTable.sortOrder().fields().stream()
                .anyMatch(field -> field.sourceId() == fieldId);
        if (isSortColumn) {
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop sort field: " + name);
        }
        int currentSpecId = icebergTable.spec().specId();
        boolean columnUsedInOlderPartitionSpecs = icebergTable.specs().entrySet().stream()
                .filter(spec -> spec.getValue().specId() != currentSpecId)
                .flatMap(spec -> spec.getValue().fields().stream())
                .anyMatch(field -> field.sourceId() == fieldId);
        if (columnUsedInOlderPartitionSpecs) {
            // After dropping a column which was used in older partition specs, insert/update/select fails on the table.
            // So restricting user to dropping that column. https://github.com/trinodb/trino/issues/15729
            throw new TrinoException(NOT_SUPPORTED, "Cannot drop column which is used by an old partition spec: " + name);
        }
        try {
            icebergTable.updateSchema()
                    .deleteColumn(name)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergColumnHandle columnHandle = (IcebergColumnHandle) source;
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        try {
            icebergTable.updateSchema()
                    .renameColumn(columnHandle.getName(), target)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to rename column: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void renameField(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, String target)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        String parentPath = String.join(".", fieldPath.subList(0, fieldPath.size() - 1));
        NestedField parent = icebergTable.schema().caseInsensitiveFindField(parentPath);

        String caseSensitiveParentName = icebergTable.schema().findColumnName(parent.fieldId());
        NestedField source = parent.type().asStructType().caseInsensitiveField(getLast(fieldPath));

        String sourcePath = caseSensitiveParentName + "." + source.name();
        try {
            icebergTable.updateSchema()
                    .renameColumn(sourcePath, target)
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to rename field: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void setColumnType(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle, io.trino.spi.type.Type type)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;
        verify(column.isBaseColumn(), "Cannot change nested field types");

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Type sourceType = icebergTable.schema().findType(column.getName());
        AtomicInteger nextFieldId = new AtomicInteger(1);
        Type newType = toIcebergTypeForNewColumn(type, nextFieldId);
        try {
            UpdateSchema schemaUpdate = icebergTable.updateSchema();
            buildUpdateSchema(column.getName(), sourceType, newType, schemaUpdate);
            schemaUpdate.commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set column type: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    private static void buildUpdateSchema(String name, Type sourceType, Type newType, UpdateSchema schemaUpdate)
    {
        if (sourceType.equals(newType)) {
            return;
        }
        if (sourceType.isPrimitiveType() && newType.isPrimitiveType()) {
            schemaUpdate.updateColumn(name, newType.asPrimitiveType());
            return;
        }
        if (sourceType instanceof StructType sourceRowType && newType instanceof StructType newRowType) {
            // Add, update or delete fields
            List<NestedField> fields = Streams.concat(sourceRowType.fields().stream(), newRowType.fields().stream())
                    .distinct()
                    .collect(toImmutableList());
            for (NestedField field : fields) {
                if (fieldExists(sourceRowType, field.name()) && fieldExists(newRowType, field.name())) {
                    buildUpdateSchema(name + "." + field.name(), sourceRowType.fieldType(field.name()), newRowType.fieldType(field.name()), schemaUpdate);
                }
                else if (fieldExists(newRowType, field.name())) {
                    schemaUpdate.addColumn(name, field.name(), field.type());
                }
                else {
                    schemaUpdate.deleteColumn(name + "." + field.name());
                }
            }

            // Order fields based on the new column type
            String currentName = null;
            for (NestedField field : newRowType.fields()) {
                String path = name + "." + field.name();
                if (currentName == null) {
                    schemaUpdate.moveFirst(path);
                }
                else {
                    schemaUpdate.moveAfter(path, currentName);
                }
                currentName = path;
            }

            return;
        }
        throw new IllegalArgumentException("Cannot change type from %s to %s".formatted(sourceType, newType));
    }

    private static boolean fieldExists(StructType structType, String fieldName)
    {
        for (NestedField field : structType.fields()) {
            if (field.name().equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setFieldType(ConnectorSession session, ConnectorTableHandle tableHandle, List<String> fieldPath, io.trino.spi.type.Type type)
    {
        Table icebergTable = catalog.loadTable(session, ((IcebergTableHandle) tableHandle).getSchemaTableName());
        String parentPath = String.join(".", fieldPath.subList(0, fieldPath.size() - 1));
        NestedField parent = icebergTable.schema().caseInsensitiveFindField(parentPath);

        String caseSensitiveParentName = icebergTable.schema().findColumnName(parent.fieldId());

        Types.StructType structType;
        if (parent.type().isListType()) {
            // list(struct...)
            structType = parent.type().asListType().elementType().asStructType();
            caseSensitiveParentName += ".element";
        }
        else {
            // just struct
            structType = parent.type().asStructType();
        }
        NestedField field = structType.caseInsensitiveField(getLast(fieldPath));

        // TODO: Add support for changing non-primitive field type
        if (!field.type().isPrimitiveType()) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg doesn't support changing field type from non-primitive types");
        }

        String name = caseSensitiveParentName + "." + field.name();
        // Pass dummy AtomicInteger. The field id will be discarded because the subsequent logic disallows non-primitive types.
        Type icebergType = toIcebergTypeForNewColumn(type, new AtomicInteger());
        if (!icebergType.isPrimitiveType()) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg doesn't support changing field type to non-primitive types");
        }
        try {
            icebergTable.updateSchema()
                    .updateColumn(name, icebergType.asPrimitiveType())
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to set field type: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public void dropNotNullConstraint(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        IcebergColumnHandle column = (IcebergColumnHandle) columnHandle;

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        verify(column.isBaseColumn(), "Cannot drop a not null constraint on nested fields");

        try {
            icebergTable.updateSchema()
                    .makeColumnOptional(column.getName())
                    .commit();
        }
        catch (RuntimeException e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to drop a not null constraint: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean tableReplace)
    {
        if (!isExtendedStatisticsEnabled(session) || !isCollectExtendedStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }

        if (tableReplace) {
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        ConnectorTableHandle tableHandle = getTableHandle(session, tableMetadata.getTable(), Optional.empty(), Optional.empty());
        if (tableHandle == null) {
            // Assume new table (CTAS), collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        if (table.getSnapshotId().isEmpty()) {
            // Table has no data (empty, or wiped out). Collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        long snapshotId = table.getSnapshotId().orElseThrow();
        Snapshot snapshot = icebergTable.snapshot(snapshotId);
        String totalRecords = snapshot.summary().get(TOTAL_RECORDS_PROP);
        if (totalRecords != null && Long.parseLong(totalRecords) == 0) {
            // Table has no data (empty, or wiped out). Collect NDV stats on all columns
            return getStatisticsCollectionMetadata(tableMetadata, Optional.empty(), availableColumnNames -> {});
        }

        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        List<IcebergColumnHandle> columns = getTopLevelColumns(schema, typeManager);
        Set<Integer> columnIds = columns.stream()
                .map(IcebergColumnHandle::getId)
                .collect(toImmutableSet());
        Map<Integer, Long> ndvs = readNdvs(icebergTable, snapshotId, columnIds, true);
        // Avoid collecting NDV stats on columns where we don't know the existing NDV count
        Set<String> columnsWithExtendedStatistics = columns.stream()
                .filter(column -> ndvs.containsKey(column.getId()))
                .map(IcebergColumnHandle::getName)
                .collect(toImmutableSet());
        return getStatisticsCollectionMetadata(tableMetadata, Optional.of(columnsWithExtendedStatistics), availableColumnNames -> {});
    }

    @Override
    public ConnectorAnalyzeMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, Map<String, Object> analyzeProperties)
    {
        IcebergTableHandle handle = checkValidTableHandle(tableHandle);
        if (!isExtendedStatisticsEnabled(session)) {
            throw new TrinoException(NOT_SUPPORTED, "Analyze is not enabled. You can enable analyze using %s config or %s catalog session property".formatted(
                    IcebergConfig.EXTENDED_STATISTICS_CONFIG,
                    IcebergSessionProperties.EXTENDED_STATISTICS_ENABLED));
        }

        checkArgument(handle.getTableType() == DATA, "Cannot analyze non-DATA table: %s", handle.getTableType());

        if (handle.getSnapshotId().isEmpty()) {
            // No snapshot, table is empty
            return new ConnectorAnalyzeMetadata(tableHandle, TableStatisticsMetadata.empty());
        }

        ConnectorTableMetadata tableMetadata = getTableMetadata(session, handle);
        Optional<Set<String>> analyzeColumnNames = getColumnNames(analyzeProperties)
                .map(columnNames -> {
                    // validate that proper column names are passed via `columns` analyze property
                    if (columnNames.isEmpty()) {
                        throw new TrinoException(INVALID_ANALYZE_PROPERTY, "Cannot specify empty list of columns for analysis");
                    }
                    return columnNames;
                });

        return new ConnectorAnalyzeMetadata(
                handle.forAnalyze(),
                getStatisticsCollectionMetadata(
                        tableMetadata,
                        analyzeColumnNames,
                        availableColumnNames -> {
                            throw new TrinoException(
                                    INVALID_ANALYZE_PROPERTY,
                                    format("Invalid columns specified for analysis: %s", Sets.difference(analyzeColumnNames.orElseThrow(), availableColumnNames)));
                        }));
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(
            ConnectorTableMetadata tableMetadata,
            Optional<Set<String>> selectedColumnNames,
            Consumer<Set<String>> unsatisfiableSelectedColumnsHandler)
    {
        Set<String> allScalarColumnNames = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .filter(column -> {
                    io.trino.spi.type.Type type = column.getType();
                    return !(type instanceof MapType || type instanceof ArrayType || type instanceof RowType); // is scalar type
                })
                .map(ColumnMetadata::getName)
                .collect(toImmutableSet());

        selectedColumnNames.ifPresent(columnNames -> {
            if (!allScalarColumnNames.containsAll(columnNames)) {
                unsatisfiableSelectedColumnsHandler.accept(allScalarColumnNames);
            }
        });

        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> allScalarColumnNames.contains(columnMetadata.getName()))
                .filter(selectedColumnNames
                        .map(columnNames -> (Predicate<ColumnMetadata>) columnMetadata -> columnNames.contains(columnMetadata.getName()))
                        .orElse(columnMetadata -> true))
                .map(column -> new ColumnStatisticMetadata(column.getName(), NUMBER_OF_DISTINCT_VALUES_NAME, NUMBER_OF_DISTINCT_VALUES_FUNCTION))
                .collect(toImmutableSet());

        return new TableStatisticsMetadata(columnStatistics, ImmutableSet.of(), ImmutableList.of());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());
        if (isS3Tables(icebergTable.location())) {
            throw new TrinoException(NOT_SUPPORTED, "S3 Tables do not support analyze");
        }
        beginTransaction(icebergTable);
        return handle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        Table table = transaction.table();
        if (handle.getSnapshotId().isEmpty()) {
            // No snapshot, table is empty
            verify(
                    computedStatistics.size() == 1,
                    "The computedStatistics size must be 1: %s",
                    computedStatistics);
            ComputedStatistics statistics = getOnlyElement(computedStatistics);
            verify(statistics.getGroupingColumns().isEmpty() &&
                            statistics.getGroupingValues().isEmpty() &&
                            statistics.getColumnStatistics().isEmpty() &&
                            statistics.getTableStatistics().isEmpty(),
                    "Unexpected non-empty statistics that cannot be attached to a snapshot because none exists: %s",
                    computedStatistics);

            commitTransaction(transaction, "statistics collection");
            transaction = null;
            return;
        }
        long snapshotId = handle.getSnapshotId().orElseThrow();

        CollectedStatistics collectedStatistics = processComputedTableStatistics(table, computedStatistics);
        StatisticsFile statisticsFile = tableStatisticsWriter.writeStatisticsFile(
                session,
                table,
                snapshotId,
                REPLACE,
                collectedStatistics);
        transaction.updateStatistics()
                .setStatistics(statisticsFile)
                .commit();

        commitTransaction(transaction, "statistics collection");
        transaction = null;
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        TupleDomain<IcebergColumnHandle> metadataColumnPredicate = table.getEnforcedPredicate().filter((column, domain) -> isMetadataColumnId(column.getId()));
        if (!metadataColumnPredicate.isAll()) {
            return Optional.empty();
        }

        // Check if the table uses Copy-on-Write mode for deletes
        // CoW deletes must go through the standard write path (finishWrite) to handle file rewriting
        // Only MoR deletes can use the optimized executeDelete path
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Map<String, String> properties = icebergTable.properties();
        UpdateMode writeDeleteMode = UpdateMode.fromIcebergProperty(properties.getOrDefault(
                DELETE_MODE, UpdateMode.MERGE_ON_READ.getIcebergProperty()));

        if (writeDeleteMode == UpdateMode.COPY_ON_WRITE) {
            // For CoW deletes, return empty to force use of standard write path (finishWrite)
            // This prevents Trino from trying to use executeDelete, which doesn't support CoW
            // The UpdateKind will be set automatically in finishWrite based on the operation type
            return Optional.empty();
        }

        // For MoR deletes, return the handle with UpdateKind set
        // This allows executeDelete to be called if applicable
        IcebergTableHandle tableWithUpdateKind = table.withUpdateKind(UpdateKind.DELETE);
        return Optional.of(tableWithUpdateKind);
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return DELETE_ROW_AND_INSERT_ROW;
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StructType type = StructType.of(ImmutableList.<NestedField>builder()
                .add(MetadataColumns.FILE_PATH)
                .add(MetadataColumns.ROW_POSITION)
                .add(NestedField.required(TRINO_MERGE_PARTITION_SPEC_ID, "partition_spec_id", IntegerType.get()))
                .add(NestedField.required(TRINO_MERGE_PARTITION_DATA, "partition_data", StringType.get()))
                .build());

        NestedField field = NestedField.required(TRINO_MERGE_ROW_ID, TRINO_ROW_ID_NAME, type);
        return getColumnHandle(field, typeManager);
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        // Set UpdateKind for UPDATE operations
        IcebergTableHandle tableWithUpdateKind = table.withUpdateKind(UpdateKind.UPDATE);

        return getInsertLayout(session, tableWithUpdateKind)
                .flatMap(ConnectorTableLayout::getPartitioning)
                .map(IcebergPartitioningHandle.class::cast)
                .map(IcebergPartitioningHandle::forUpdate);
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        verifyTableVersionForUpdate(table);

        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        validateNotModifyingOldSnapshot(table, icebergTable);

        beginTransaction(icebergTable);

        // Set the UpdateKind to MERGE for this operation
        IcebergTableHandle tableHandleWithUpdateKind = table.withUpdateKind(UpdateKind.MERGE);

        IcebergWritableTableHandle insertHandle = newWritableTableHandle(tableHandleWithUpdateKind.getSchemaTableName(), icebergTable);
        return new IcebergMergeTableHandle(tableHandleWithUpdateKind, insertHandle);
    }

    @Override
    public void finishMerge(ConnectorSession session, ConnectorMergeTableHandle mergeTableHandle, List<ConnectorTableHandle> sourceTableHandles, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        IcebergMergeTableHandle mergeHandle = (IcebergMergeTableHandle) mergeTableHandle;
        IcebergTableHandle handle = mergeHandle.getTableHandle();
        finishWrite(session, handle, fragments);
    }

    private static void verifyTableVersionForUpdate(IcebergTableHandle table)
    {
        if (table.getFormatVersion() < 2) {
            throw new TrinoException(NOT_SUPPORTED, "Iceberg table updates require at least format version 2");
        }
    }

    private static void validateNotModifyingOldSnapshot(IcebergTableHandle table, Table icebergTable)
    {
        Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (table.getSnapshotId().isPresent() &&
                (currentSnapshot == null || table.getSnapshotId().get() != currentSnapshot.snapshotId())) {
            throw new TrinoException(NOT_SUPPORTED, "Modifying old snapshot is not supported in Iceberg");
        }
    }

    private void finishWrite(ConnectorSession session, IcebergTableHandle table, Collection<Slice> fragments)
            throws IllegalStateException
    {
        Table icebergTable = transaction.table();

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        if (commitTasks.isEmpty()) {
            // Avoid recording "empty" write operation
            transaction = null;
            return;
        }

        // Detect operation type if not already set:
        // - UPDATE: has both POSITION_DELETES and DATA tasks (DELETE_ROW_AND_INSERT_ROW pattern)
        // - DELETE: has only POSITION_DELETES tasks (no DATA tasks)
        // - MERGE: already set in beginMerge()
        UpdateKind detectedUpdateKind = table.getUpdateKind().orElse(null);
        IcebergTableHandle tableHandle = table;
        if (detectedUpdateKind == null) {
            boolean hasDeletes = commitTasks.stream().anyMatch(task -> task.content() == FileContent.POSITION_DELETES);
            boolean hasData = commitTasks.stream().anyMatch(task -> task.content() == FileContent.DATA);
            if (hasDeletes && hasData) {
                // This is an UPDATE operation (DELETE + INSERT pattern)
                detectedUpdateKind = UpdateKind.UPDATE;
                tableHandle = table.withUpdateKind(UpdateKind.UPDATE);
            }
            else if (hasDeletes && !hasData) {
                // This is a DELETE operation (only POSITION_DELETES, no DATA)
                detectedUpdateKind = UpdateKind.DELETE;
                tableHandle = table.withUpdateKind(UpdateKind.DELETE);
            }
        }

        Schema schema = SchemaParser.fromJson(tableHandle.getTableSchemaJson());

        // Determine the update mode to use based on the update kind and table properties
        UpdateMode updateMode = UpdateMode.MERGE_ON_READ; // Default to MOR
        Map<String, String> icebergProperties = icebergTable.properties();

        if (detectedUpdateKind != null) {
            switch (detectedUpdateKind) {
                case DELETE:
                    // Read from Iceberg property (stored as DELETE_MODE) with fallback to default
                    updateMode = UpdateMode.fromIcebergProperty(icebergProperties.getOrDefault(
                            DELETE_MODE,
                            UpdateMode.MERGE_ON_READ.getIcebergProperty()));
                    break;
                case UPDATE:
                    // Read from Iceberg property (stored as UPDATE_MODE) with fallback to default
                    updateMode = UpdateMode.fromIcebergProperty(icebergProperties.getOrDefault(
                            UPDATE_MODE,
                            UpdateMode.MERGE_ON_READ.getIcebergProperty()));
                    break;
                case MERGE:
                    // Read from Iceberg property (stored as MERGE_MODE) with fallback to default
                    updateMode = UpdateMode.fromIcebergProperty(icebergProperties.getOrDefault(
                            MERGE_MODE,
                            UpdateMode.MERGE_ON_READ.getIcebergProperty()));
                    break;
                default:
                    updateMode = UpdateMode.MERGE_ON_READ;
            }
        }

        // Create the appropriate update operation based on the update mode
        RowDelta rowDelta = null;
        RewriteFiles rewriteFiles = null;

        final IcebergTableHandle finalTableHandle = tableHandle;
        Optional<Long> snapshotId = finalTableHandle.getSnapshotId();
        // For MERGE with CoW, we use a special two-phase commit approach, so don't create rewriteFiles
        // For other CoW operations (DELETE, UPDATE), use RewriteFiles
        if (updateMode == UpdateMode.COPY_ON_WRITE && detectedUpdateKind != null && detectedUpdateKind != UpdateKind.MERGE) {
            // For Copy-on-Write mode (except MERGE), use RewriteFiles
            RewriteFiles localRewriteFiles = transaction.newRewrite();
            snapshotId.map(icebergTable::snapshot).ifPresent(s -> localRewriteFiles.validateFromSnapshot(s.snapshotId()));
            rewriteFiles = localRewriteFiles;
        }
        else if (updateMode == UpdateMode.COPY_ON_WRITE && detectedUpdateKind == UpdateKind.MERGE) {
            // For MERGE with CoW, we'll use RowDelta in the second phase, but don't create it here yet
            // We'll create it after committing the first transaction
            rowDelta = null;
            rewriteFiles = null;
        }
        else {
            // For Merge-on-Read mode or when update kind is not specified, use RowDelta
            RowDelta localRowDelta = transaction.newRowDelta();
            snapshotId.map(icebergTable::snapshot).ifPresent(s -> localRowDelta.validateFromSnapshot(s.snapshotId()));
            rowDelta = localRowDelta;
        }
        TupleDomain<IcebergColumnHandle> dataColumnPredicate = finalTableHandle.getEnforcedPredicate().filter((column, domain) -> !isMetadataColumnId(column.getId()));
        TupleDomain<IcebergColumnHandle> effectivePredicate = dataColumnPredicate.intersect(finalTableHandle.getUnenforcedPredicate());
        if (isFileBasedConflictDetectionEnabled(session)) {
            effectivePredicate = effectivePredicate.intersect(extractTupleDomainsFromCommitTasks(finalTableHandle, icebergTable, commitTasks, typeManager));
        }

        effectivePredicate = effectivePredicate.filter((_, domain) -> isConvertibleToIcebergExpression(domain));

        if (!effectivePredicate.isAll()) {
            if (rowDelta != null) {
                rowDelta.conflictDetectionFilter(toIcebergExpression(effectivePredicate));
            }
            // For RewriteFiles, we don't set a conflict detection filter as it works differently
        }

        IsolationLevel isolationLevel = IsolationLevel.fromName(icebergTable.properties().getOrDefault(DELETE_ISOLATION_LEVEL, DELETE_ISOLATION_LEVEL_DEFAULT));

        if (rowDelta != null) {
            if (isolationLevel == IsolationLevel.SERIALIZABLE) {
                rowDelta.validateNoConflictingDataFiles();
            }

            // Ensure a row that is updated by this commit was not deleted by a separate commit
            rowDelta.validateDeletedFiles();
            rowDelta.validateNoConflictingDeleteFiles();
            rowDelta.scanManifestsWith(icebergScanExecutor);
        }
        else if (rewriteFiles != null) {
            // For RewriteFiles, we'll configure scanning behavior
            rewriteFiles.scanManifestsWith(icebergScanExecutor);
        }

        // In CoW mode, we need to track both files to add and files to delete/rewrite
        Map<String, DataFile> filesToAdd = new HashMap<>();
        Set<String> filesToDelete = new HashSet<>();
        ImmutableSet.Builder<String> referencedDataFiles = ImmutableSet.builder();
        // For CoW DELETE operations, track delete files so we can rewrite data files to exclude deleted rows
        Map<String, List<DeleteFile>> deleteFilesByDataFile = new HashMap<>();

        // First, process all tasks to build the sets of files to add and delete
        for (CommitTaskData task : commitTasks) {
            PartitionSpec partitionSpec = PartitionSpecParser.fromJson(schema, task.partitionSpecJson());
            Type[] partitionColumnTypes = partitionSpec.fields().stream()
                    .map(field -> field.transform().getResultType(schema.findType(field.sourceId())))
                    .toArray(Type[]::new);

            switch (task.content()) {
                case POSITION_DELETES -> {
                    if (rowDelta != null) {
                        // For MOR mode, add deletes as usual
                        FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(partitionSpec)
                                .withPath(task.path())
                                .withFormat(task.fileFormat().toIceberg())
                                .ofPositionDeletes()
                                .withFileSizeInBytes(task.fileSizeInBytes())
                                .withMetrics(task.metrics().metrics());
                        task.fileSplitOffsets().ifPresent(deleteBuilder::withSplitOffsets);
                        if (!partitionSpec.fields().isEmpty()) {
                            String partitionDataJson = task.partitionDataJson()
                                    .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                            deleteBuilder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                        }
                        rowDelta.addDeletes(deleteBuilder.build());
                        task.referencedDataFile().ifPresent(referencedDataFiles::add);
                    }
                    else if (rewriteFiles != null || (updateMode == UpdateMode.COPY_ON_WRITE && (detectedUpdateKind == UpdateKind.MERGE || detectedUpdateKind == UpdateKind.UPDATE || detectedUpdateKind == UpdateKind.DELETE))) {
                        // For CoW mode (DELETE, UPDATE, MERGE), mark the referenced data file for deletion
                        // It will be replaced by a rewritten file that doesn't contain the deleted rows
                        String referencedDataFile = task.referencedDataFile().orElse(null);
                        if (referencedDataFile != null) {
                            filesToDelete.add(referencedDataFile);
                            // Build the delete file to track which rows need to be excluded when rewriting
                            FileMetadata.Builder deleteBuilder = FileMetadata.deleteFileBuilder(partitionSpec)
                                    .withPath(task.path())
                                    .withFormat(task.fileFormat().toIceberg())
                                    .ofPositionDeletes()
                                    .withFileSizeInBytes(task.fileSizeInBytes())
                                    .withMetrics(task.metrics().metrics());
                            task.fileSplitOffsets().ifPresent(deleteBuilder::withSplitOffsets);
                            if (!partitionSpec.fields().isEmpty()) {
                                String partitionDataJson = task.partitionDataJson()
                                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                                deleteBuilder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                            }
                            DeleteFile deleteFile = deleteBuilder.build();
                            deleteFilesByDataFile.computeIfAbsent(referencedDataFile, k -> new ArrayList<>()).add(deleteFile);
                        }
                    }
                }
                case DATA -> {
                    DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                            .withPath(task.path())
                            .withFormat(task.fileFormat().toIceberg())
                            .withFileSizeInBytes(task.fileSizeInBytes())
                            .withMetrics(task.metrics().metrics());
                    if (!icebergTable.spec().fields().isEmpty()) {
                        String partitionDataJson = task.partitionDataJson()
                                .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                        builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
                    }

                    DataFile dataFile = builder.build();
                    if (rowDelta != null) {
                        // For MOR mode, add new rows as usual
                        rowDelta.addRows(dataFile);
                    }
                    else if (rewriteFiles != null || (updateMode == UpdateMode.COPY_ON_WRITE && (detectedUpdateKind == UpdateKind.MERGE || detectedUpdateKind == UpdateKind.UPDATE || detectedUpdateKind == UpdateKind.DELETE))) {
                        // For CoW mode (DELETE, UPDATE, MERGE), store this file to be added later
                        filesToAdd.put(task.path(), dataFile);
                    }
                }
                default -> throw new UnsupportedOperationException("Unsupported task content: " + task.content());
            }
        }

        if (rowDelta != null) {
            // For MOR mode
            rowDelta.validateDataFilesExist(referencedDataFiles.build());
            commitUpdateAndTransaction(rowDelta, session, transaction, "write");
        }
        else if (rewriteFiles != null || (updateMode == UpdateMode.COPY_ON_WRITE && (detectedUpdateKind == UpdateKind.MERGE || detectedUpdateKind == UpdateKind.UPDATE || detectedUpdateKind == UpdateKind.DELETE))) {
            // For CoW mode
            // Optimized approach: Read manifests directly instead of scanning all files
            // This is much more efficient for large tables as we only read manifest metadata
            // instead of creating FileScanTasks for every data file in the table.
            // Performance improvement: O(manifest_count) instead of O(data_file_count)
            // Snapshot isolation ensures the snapshot we read from remains consistent
            // throughout this operation, protecting against concurrent modifications
            if (!filesToDelete.isEmpty()) {
                // Build an immutable set for fast O(1) lookup
                Set<String> filesToDeleteSet = ImmutableSet.copyOf(filesToDelete);
                int remainingFiles = filesToDeleteSet.size();

                // Read manifests from current snapshot to get DataFile objects for files to delete
                Snapshot currentSnapshot = icebergTable.currentSnapshot();
                if (currentSnapshot == null) {
                    // This should not happen: if we have files to delete, the table must have a snapshot
                    // This indicates an internal consistency error in the transaction state
                    throw new TrinoException(ICEBERG_INVALID_METADATA,
                            format("Internal error: Table has no current snapshot but %d file(s) are marked for deletion. " +
                                   "This indicates a problem with the transaction state.", filesToDelete.size()));
                }

                List<ManifestFile> manifests = loadAllManifestsFromSnapshot(icebergTable, currentSnapshot);

                // Read data files from manifests, filtering by paths we need
                for (ManifestFile manifest : manifests) {
                    // Only read data manifests (not delete manifests)
                    if (manifest.content() != ManifestContent.DATA) {
                        continue;
                    }

                    // Early termination: if we've found all files, stop reading manifests
                    if (remainingFiles == 0) {
                        break;
                    }

                    try (ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifest, icebergTable.io())) {
                        for (DataFile dataFile : manifestReader) {
                            String path = dataFile.path().toString();

                            if (filesToDeleteSet.contains(path)) {
                                // Add this file to be deleted (skip for MERGE as we handle it differently)
                                if (rewriteFiles != null) {
                                    rewriteFiles.deleteFile(dataFile);
                                }

                                // Track that this file is being replaced
                                referencedDataFiles.add(path);

                                remainingFiles--;
                                // Early termination within manifest if all files found
                                if (remainingFiles == 0) {
                                    break;
                                }
                            }
                        }
                    }
                    catch (IOException e) {
                        throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
                                "Error reading manifest file: " + manifest.path(), e);
                    }
                }

                // Validate that all files to delete were found in the table's manifests
                if (remainingFiles > 0) {
                    throw new TrinoException(ICEBERG_INVALID_METADATA,
                            format("Could not find %d file(s) to delete in table manifests for Copy-on-Write operation. " +
                                    "The files may have been removed by a concurrent operation. " +
                                    "Table: %s", remainingFiles, icebergTable.name()));
                }
            }

            // For CoW DELETE, UPDATE, and MERGE operations, if we have files to delete,
            // we need to rewrite the data files to exclude deleted rows
            // For DELETE: filesToAdd will be empty (only rewriting)
            // For UPDATE: filesToAdd will contain new data files with updated rows (these should be kept)
            //   The rewrite creates files with non-updated rows, and new data files contain updated rows
            //   Both should be added - rewritten files for non-updated rows, new data files for updated rows
            // For MERGE: filesToAdd may contain new files (insertions), but we still need to rewrite files with deletes
            Set<String> rewrittenFilePaths = new HashSet<>();
            if ((detectedUpdateKind == UpdateKind.DELETE || detectedUpdateKind == UpdateKind.UPDATE || detectedUpdateKind == UpdateKind.MERGE)
                    && updateMode == UpdateMode.COPY_ON_WRITE && !filesToDelete.isEmpty()) {
                // Rewrite data files to exclude deleted rows
                rewriteDataFilesForCowDelete(session, icebergTable, schema, filesToDelete, deleteFilesByDataFile, filesToAdd, rewrittenFilePaths);
            }

            // Validate that CoW DELETE operations have rewritten files after the rewrite attempt
            // Note: MERGE operations may have filesToAdd from insertions, so we only validate DELETE operations
            if (detectedUpdateKind == UpdateKind.DELETE && updateMode == UpdateMode.COPY_ON_WRITE && filesToAdd.isEmpty() && !filesToDelete.isEmpty()) {
                // After attempting to rewrite, if we still have no files to add, this is an error
                throw new TrinoException(NOT_SUPPORTED,
                        format("Copy-on-Write DELETE operations require file rewriting, but no rewritten files were generated for table %s. " +
                               "This may indicate that the DELETE operation is not properly configured for CoW mode.", icebergTable.name()));
            }

            // Now add all the new files (skip for MERGE as we handle it in the second phase)
            // For UPDATE operations in CoW mode, we need to be careful:
            // - Rewritten files contain non-updated rows (excludes deleted rows)
            // - New data files contain updated rows
            // Both should be added, but we need to make sure we're not duplicating
            if (rewriteFiles != null) {
                for (DataFile dataFile : filesToAdd.values()) {
                    rewriteFiles.addFile(dataFile);
                }
            }

            // Set dataSequenceNumber to avoid contention with concurrent operations
            // This is important for CoW operations to ensure proper snapshot isolation
            // Use the snapshot we validated from, or current snapshot if no validation was set
            Snapshot snapshotForSequence = snapshotId.map(icebergTable::snapshot).orElse(icebergTable.currentSnapshot());
            if (snapshotForSequence != null && rewriteFiles != null) {
                rewriteFiles.dataSequenceNumber(snapshotForSequence.sequenceNumber());
            }

            if (detectedUpdateKind == UpdateKind.MERGE && updateMode == UpdateMode.COPY_ON_WRITE) {
                // For MERGE operations with copy-on-write mode, we need to handle the operation differently
                // Instead of using RewriteFiles, which triggers Iceberg's validation that prevents MERGE operations
                // that legitimately add more records than they replace, we'll use a new transaction approach
                // This is a special case just for MERGE operations with CoW mode
                // Note: rewriteFiles was not created for MERGE, so we can commit the transaction directly
                // The position delete files are already written and committed by PositionDeleteWriter
                // Commit the current transaction (this commits the position delete files)
                commitTransaction(transaction, "merge with cow - prepare");
                // Clear the transaction field so we can start a new one
                transaction = null;
                // Reload the table to get the latest state after the first commit
                icebergTable = catalog.loadTable(session, tableHandle.getSchemaTableName());
                // Create a new transaction with the refreshed table
                beginTransaction(icebergTable);
                // Create a fresh RowDelta in the new transaction
                RowDelta specialMergeDelta = transaction.newRowDelta();

                // Configure RowDelta similar to how we configure it for MoR mode
                // Re-use the isolationLevel variable from above
                if (isolationLevel == IsolationLevel.SERIALIZABLE) {
                    specialMergeDelta.validateNoConflictingDataFiles();
                }
                specialMergeDelta.validateDeletedFiles();
                specialMergeDelta.validateNoConflictingDeleteFiles();
                specialMergeDelta.scanManifestsWith(icebergScanExecutor);

                // For MERGE with CoW, we've already rewritten files to exclude deleted rows
                // The rewritten files are in filesToAdd and replace the original files
                // We need to remove the original files that were rewritten and add the new rewritten files
                // Only remove files that were actually rewritten (have entries in rewrittenFilePaths)
                for (String path : filesToDelete) {
                    if (rewrittenFilePaths.contains(path)) {
                        // This file was rewritten - remove the old file and add the rewritten one
                        DataFile file = getFileFromManifest(icebergTable, path);
                        if (file != null) {
                            // Remove all rows from the old file (it's being replaced by the rewritten file)
                            specialMergeDelta.removeRows(file);
                        }
                    }
                    // If the file wasn't rewritten, it means it had no deletes, so we don't need to remove it
                }

                // Then add all the new files (including rewritten files from CoW operations)
                for (DataFile file : filesToAdd.values()) {
                    // Add new data files to the delta (includes rewritten files and new insertions)
                    specialMergeDelta.addRows(file);
                }

                // Note: In CoW mode, position delete files are consumed during the rewrite process
                // and should NOT be added to the transaction. The rewritten data files already exclude
                // the deleted rows, so we don't need to track the delete files separately.

                // Set data sequence number just like we do for RewriteFiles
                // Re-use the snapshotForSequence variable from above
                if (snapshotForSequence != null) {
                    // Use the correct method for setting sequence number
                    specialMergeDelta.validateFromSnapshot(snapshotForSequence.snapshotId());
                }

                // Use RowDelta for the commit instead of RewriteFiles to bypass the validation
                // Use commitUpdateAndTransaction to ensure proper transaction tracking (same as MoR mode)
                commitUpdateAndTransaction(specialMergeDelta, session, transaction, "merge with cow");
            }
            else {
                // For other operations (DELETE, UPDATE), use standard REPLACE semantics with RewriteFiles
                commitUpdateAndTransaction(rewriteFiles, session, transaction, "write");
            }
        }
        // Update done!
    }

    /**
     * Retrieves a DataFile object from the table's current manifests by matching the file path.
     *
     * @param table The Iceberg table
     * @param path The path of the file to find
     * @return The DataFile if found, null otherwise
     */
    private static DataFile getFileFromManifest(Table table, String path)
    {
        if (table.currentSnapshot() == null) {
            // No snapshot yet
            return null;
        }
        List<ManifestFile> dataManifests = table.currentSnapshot().dataManifests(table.io());
        for (ManifestFile manifest : dataManifests) {
            try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io())) {
                // Use the reader iterator directly as scan() method may not exist
                for (DataFile file : reader) {
                    if (file.path().equals(path)) {
                        return file;
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return null;
    }

    /**
     * Rewrites data files for CoW DELETE operations by reading the original data files,
     * applying position deletes to filter out deleted rows, and writing new data files.
     * <p>
     * This method handles the Copy-on-Write rewrite logic for DELETE operations where
     * we need to create new data files that exclude the rows specified in delete files.
     * <p>
     * The implementation:
     * 1. For each data file to be rewritten, reads the position delete files
     * 2. Reads the original data file
     * 3. Filters out rows at positions specified in the delete files
     * 4. Writes a new data file with the remaining rows
     * 5. Adds the new file to filesToAdd for inclusion in the RewriteFiles operation
     *
     * @param session The connector session
     * @param icebergTable The Iceberg table
     * @param schema The table schema
     * @param filesToDelete Set of data file paths that need to be rewritten
     * @param deleteFilesByDataFile Map of data file paths to their associated delete files
     * @param filesToAdd Map to add newly created data files (output parameter)
     */
    private void rewriteDataFilesForCowDelete(
            ConnectorSession session,
            Table icebergTable,
            Schema schema,
            Set<String> filesToDelete,
            Map<String, List<DeleteFile>> deleteFilesByDataFile,
            Map<String, DataFile> filesToAdd,
            Set<String> rewrittenFilePaths)
    {
        // Get the current snapshot to read data files from
        Snapshot currentSnapshot = icebergTable.currentSnapshot();
        if (currentSnapshot == null) {
            throw new TrinoException(ICEBERG_INVALID_METADATA,
                    "Cannot rewrite files: table has no current snapshot");
        }

        // Load all manifests to find the DataFile objects for files we need to rewrite
        List<ManifestFile> manifests = loadAllManifestsFromSnapshot(icebergTable, currentSnapshot);
        Map<String, DataFile> dataFileMap = new HashMap<>();

        // Build a map of file paths to DataFile objects
        for (ManifestFile manifest : manifests) {
            if (manifest.content() != ManifestContent.DATA) {
                continue;
            }
            try (ManifestReader<DataFile> manifestReader = ManifestFiles.read(manifest, icebergTable.io())) {
                for (DataFile dataFile : manifestReader) {
                    String path = dataFile.path().toString();
                    if (filesToDelete.contains(path)) {
                        dataFileMap.put(path, dataFile);
                    }
                }
            }
            catch (IOException e) {
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
                        "Error reading manifest file: " + manifest.path(), e);
            }
        }

        // For each data file that needs to be rewritten
        for (String dataFilePath : filesToDelete) {
            DataFile originalDataFile = dataFileMap.get(dataFilePath);
            if (originalDataFile == null) {
                throw new TrinoException(ICEBERG_INVALID_METADATA,
                        "Data file not found in manifests: " + dataFilePath);
            }

            List<DeleteFile> deleteFiles = deleteFilesByDataFile.getOrDefault(dataFilePath, ImmutableList.of());
            if (deleteFiles.isEmpty()) {
                // No delete files for this data file - skip rewriting
                // This shouldn't happen, but handle gracefully
                continue;
            }

            // Read position deletes to get the set of row positions to exclude
            // Use the same bitmap implementation as DeleteManager
            org.roaringbitmap.longlong.Roaring64Bitmap deletedPositions = new org.roaringbitmap.longlong.Roaring64Bitmap();
            for (DeleteFile deleteFile : deleteFiles) {
                if (deleteFile.content() != FileContent.POSITION_DELETES) {
                    // Only handle position deletes for now
                    continue;
                }
                readPositionDeletesFromDeleteFile(session, icebergTable, deleteFile, dataFilePath, deletedPositions);
            }

            if (deletedPositions.isEmpty()) {
                // No rows to delete - skip rewriting this file
                continue;
            }

            // Rewrite the data file excluding deleted rows
            // Get all column handles for reading the data file
            // Use the current table schema, not the schema from the table handle, to ensure
            // partition field types are correctly resolved (schema evolution might have occurred)
            Schema currentSchema = icebergTable.schema();
            List<IcebergColumnHandle> allColumns = IcebergUtil.getTopLevelColumns(currentSchema, typeManager);
            PartitionSpec partitionSpec = icebergTable.spec();
            org.apache.iceberg.types.Type[] partitionColumnTypes = partitionSpec.fields().stream()
                    .map(field -> {
                        org.apache.iceberg.types.Type sourceType = currentSchema.findType(field.sourceId());
                        if (sourceType == null) {
                            throw new TrinoException(ICEBERG_INVALID_METADATA,
                                    format("Partition field source ID %d not found in table schema for file %s", field.sourceId(), dataFilePath));
                        }
                        return field.transform().getResultType(sourceType);
                    })
                    .toArray(org.apache.iceberg.types.Type[]::new);

            // Get partition data from the original data file
            Optional<PartitionData> partitionData = Optional.empty();
            if (!partitionSpec.isUnpartitioned() && originalDataFile.partition() != null) {
                partitionData = Optional.of(PartitionData.fromJson(
                        PartitionData.toJson(originalDataFile.partition()),
                        partitionColumnTypes));
            }

            // Generate output file path
            LocationProvider locationProvider = icebergTable.locationProvider();
            IcebergFileFormat fileFormat = IcebergFileFormat.fromIceberg(originalDataFile.format());
            String fileName = fileFormat.toIceberg().addExtension(session.getQueryId() + "-" + randomUUID());
            String outputPath = partitionData
                    .map(partition -> locationProvider.newDataLocation(partitionSpec, partition, fileName))
                    .orElseGet(() -> locationProvider.newDataLocation(fileName));

            // Create file writer
            TrinoFileSystem fileSystem = fileSystemFactory.create(session.getIdentity(), icebergTable.io().properties());
            IcebergFileWriter writer = fileWriterFactory.createDataFileWriter(
                    fileSystem,
                    Location.of(outputPath),
                    currentSchema,
                    session,
                    fileFormat,
                    MetricsConfig.getDefault(),
                    icebergTable.properties());

            // Read the data file and filter out deleted rows
            // Row positions in Iceberg are 0-based within each data file
            long currentRowPosition = 0;
            try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                    session,
                    allColumns,
                    currentSchema,
                    partitionSpec,
                    partitionData.orElse(new PartitionData(new Object[partitionSpec.fields().size()])),
                    ImmutableList.of(), // No deletes - we're reading the original file
                    DynamicFilter.EMPTY,
                    TupleDomain.all(),
                    TupleDomain.all(),
                    dataFilePath,
                    0,
                    originalDataFile.fileSizeInBytes(),
                    originalDataFile.fileSizeInBytes(),
                    originalDataFile.recordCount(),
                    partitionData.map(PartitionData::toJson).orElse(""),
                    fileFormat,
                    icebergTable.io().properties(),
                    originalDataFile.dataSequenceNumber(),
                    Optional.empty())) {
                while (!pageSource.isFinished()) {
                    SourcePage sourcePage = pageSource.getNextSourcePage();
                    if (sourcePage == null) {
                        continue;
                    }
                    Page page = sourcePage.getPage();

                    // Filter out deleted rows based on their position
                    int[] positionsToKeep = new int[page.getPositionCount()];
                    int keptCount = 0;
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        long rowPosition = currentRowPosition + position;
                        if (!deletedPositions.contains(rowPosition)) {
                            positionsToKeep[keptCount++] = position;
                        }
                    }

                    if (keptCount > 0) {
                        // Create a filtered page with only non-deleted rows
                        Block[] filteredBlocks = new Block[page.getChannelCount()];
                        for (int channel = 0; channel < page.getChannelCount(); channel++) {
                            filteredBlocks[channel] = page.getBlock(channel).copyPositions(positionsToKeep, 0, keptCount);
                        }
                        Page filteredPage = new Page(keptCount, filteredBlocks);
                        writer.appendRows(filteredPage);
                    }

                    currentRowPosition += page.getPositionCount();
                }
            }
            catch (IOException e) {
                writer.rollback();
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
                        "Error reading data file for rewriting: " + dataFilePath, e);
            }

            // Commit the writer and get metrics
            // writer.commit() returns a Closeable that represents a rollback action.
            // This should only be closed if there is a failure after the commit succeeds.
            // We keep the rollback handle until we successfully add the file to filesToAdd.
            Closeable rollback = writer.commit();

            try {
                IcebergFileWriter.FileMetrics fileMetrics = writer.getFileMetrics();

                // Create DataFile metadata for the new file
                DataFiles.Builder builder = DataFiles.builder(partitionSpec)
                        .withPath(outputPath)
                        .withFormat(originalDataFile.format())
                        .withFileSizeInBytes(writer.getWrittenBytes())
                        .withMetrics(fileMetrics.metrics());
                if (fileMetrics.splitOffsets().isPresent()) {
                    builder.withSplitOffsets(fileMetrics.splitOffsets().get());
                }
                if (partitionData.isPresent()) {
                    builder.withPartition(partitionData.get());
                }

                DataFile newDataFile = builder.build();
                filesToAdd.put(outputPath, newDataFile);
                // Track that this file was rewritten (so we know which old files to remove for MERGE)
                rewrittenFilePaths.add(dataFilePath);

                // Success - file added to transaction. The rollback will be handled
                // by the transaction rollback mechanism if the overall transaction fails.
                // We don't need to close it here as it's just a cleanup lambda with no resources.
                rollback = null;
            }
            catch (Exception e) {
                // If we fail to add the file, clean up the committed file immediately
                if (rollback != null) {
                    try {
                        rollback.close();
                    }
                    catch (IOException cleanupException) {
                        e.addSuppressed(new IOException("Failed to cleanup file after error: " + outputPath, cleanupException));
                    }
                    rollback = null;
                }
                throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
                        "Error creating DataFile metadata for rewritten file: " + outputPath, e);
            }
            finally {
                // Ensure rollback is closed if an error occurred and it wasn't already closed
                if (rollback != null) {
                    try {
                        rollback.close();
                    }
                    catch (IOException e) {
                        // Log but don't throw - this is cleanup
                        log.warn(e, "Error closing rollback handle for file: %s", outputPath);
                    }
                }
            }
        }
    }

    /**
     * Reads position deletes from a delete file and adds the deleted positions to the bitmap.
     * Only positions for the specified data file path are included.
     *
     * @param session The connector session
     * @param icebergTable The Iceberg table
     * @param deleteFile The delete file to read from
     * @param targetDataFilePath The data file path to filter deletes for
     * @param deletedPositions The bitmap to populate with deleted positions
     */
    private void readPositionDeletesFromDeleteFile(
            ConnectorSession session,
            Table icebergTable,
            DeleteFile deleteFile,
            String targetDataFilePath,
            org.roaringbitmap.longlong.Roaring64Bitmap deletedPositions)
    {
        // Get column handles for delete file columns (path and position)
        IcebergColumnHandle deleteFilePath = IcebergUtil.getColumnHandle(DELETE_FILE_PATH, typeManager);
        IcebergColumnHandle deleteFilePos = IcebergUtil.getColumnHandle(DELETE_FILE_POS, typeManager);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);

        // Create a tuple domain to filter for the target data file path
        Slice targetPath = utf8Slice(targetDataFilePath);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(
                ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));

        // Open the delete file and read position deletes
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                session,
                deleteColumns,
                IcebergUtil.schemaFromHandles(deleteColumns),
                icebergTable.spec(),
                new PartitionData(new Object[icebergTable.spec().fields().size()]),
                ImmutableList.of(), // No deletes for delete files themselves
                DynamicFilter.EMPTY,
                deleteDomain,
                TupleDomain.all(),
                deleteFile.path().toString(),
                0,
                deleteFile.fileSizeInBytes(),
                deleteFile.fileSizeInBytes(),
                0,
                "",
                IcebergFileFormat.fromIceberg(deleteFile.format()),
                icebergTable.io().properties(),
                0,
                Optional.empty())) {
            io.trino.plugin.iceberg.delete.PositionDeleteFilter.readPositionDeletes(pageSource, targetPath, deletedPositions);
        }
        catch (IOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR,
                    "Error reading delete file: " + deleteFile.path(), e);
        }
    }

    static TupleDomain<IcebergColumnHandle> extractTupleDomainsFromCommitTasks(IcebergTableHandle table, Table icebergTable, List<CommitTaskData> commitTasks, TypeManager typeManager)
    {
        Set<IcebergColumnHandle> partitionColumns = new HashSet<>(getProjectedColumns(icebergTable.schema(), typeManager, identityPartitionColumnsInAllSpecs(icebergTable)));
        PartitionSpec partitionSpec = icebergTable.spec();
        Type[] partitionColumnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);
        Schema schema = SchemaParser.fromJson(table.getTableSchemaJson());
        Map<IcebergColumnHandle, List<Domain>> domainsFromTasks = new HashMap<>();
        for (CommitTaskData commitTask : commitTasks) {
            PartitionSpec taskPartitionSpec = PartitionSpecParser.fromJson(schema, commitTask.partitionSpecJson());
            if (commitTask.partitionDataJson().isEmpty() || taskPartitionSpec.isUnpartitioned() || !taskPartitionSpec.equals(partitionSpec)) {
                // We should not produce any specific domains if there are no partitions or current partitions does not match task partitions for any of tasks
                // As each partition value narrows down conflict scope we should produce values from all commit tasks or not at all, to avoid partial information
                return TupleDomain.all();
            }

            PartitionData partitionData = PartitionData.fromJson(commitTask.partitionDataJson().get(), partitionColumnTypes);
            Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(partitionData, partitionSpec);
            Map<ColumnHandle, NullableValue> partitionValues = getPartitionValues(partitionColumns, partitionKeys);

            for (Map.Entry<ColumnHandle, NullableValue> entry : partitionValues.entrySet()) {
                IcebergColumnHandle columnHandle = (IcebergColumnHandle) entry.getKey();
                NullableValue value = entry.getValue();
                Domain newDomain = value.isNull() ? Domain.onlyNull(columnHandle.getType()) : Domain.singleValue(columnHandle.getType(), value.getValue());
                domainsFromTasks.computeIfAbsent(columnHandle, _ -> new ArrayList<>()).add(newDomain);
            }
        }
        return withColumnDomains(domainsFromTasks.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> Domain.union(entry.getValue()))));
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, Map<String, Object> viewProperties, boolean replace)
    {
        checkArgument(viewProperties.isEmpty(), "This connector does not support creating views with properties");
        catalog.createView(session, viewName, definition, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        catalog.renameView(session, source, target);
    }

    @Override
    public void setViewAuthorization(ConnectorSession session, SchemaTableName viewName, TrinoPrincipal principal)
    {
        catalog.setViewPrincipal(session, viewName, principal);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.getViews(session, schemaName);
    }

    @Override
    public boolean isView(ConnectorSession session, SchemaTableName viewName)
    {
        try {
            return catalog.getView(session, viewName).isPresent();
        }
        catch (TrinoException e) {
            if (e.getErrorCode() == ICEBERG_UNSUPPORTED_VIEW_DIALECT.toErrorCode()) {
                return true;
            }
            throw e;
        }
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getView(session, viewName);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        Table icebergTable = catalog.loadTable(session, handle.getSchemaTableName());

        // Safety check: CoW deletes must go through finishWrite, not executeDelete
        // This method is only for MoR deletes that can be done with metadata-only operations
        Map<String, String> properties = icebergTable.properties();
        UpdateMode writeDeleteMode = UpdateMode.fromIcebergProperty(properties.getOrDefault(
                DELETE_MODE, UpdateMode.MERGE_ON_READ.getIcebergProperty()));

        if (writeDeleteMode == UpdateMode.COPY_ON_WRITE) {
            throw new TrinoException(NOT_SUPPORTED,
                    format("Copy-on-Write deletes cannot use executeDelete. " +
                            "They must go through the standard write path (finishWrite) which handles file rewriting. " +
                            "Table: %s", icebergTable.name()));
        }

        // Note: Copy-on-Write deletes are handled through the standard write path (finishWrite)
        // This method is only called for Merge-on-Read deletes that can be done with metadata-only operations
        DeleteFiles update = icebergTable.newDelete()
                .deleteFromRowFilter(toIcebergExpression(handle.getEnforcedPredicate()))
                .scanManifestsWith(icebergScanExecutor);
        commitUpdate(update, session, "delete");

        Map<String, String> summary = icebergTable.currentSnapshot().summary();
        String deletedRowsStr = summary.get(DELETED_RECORDS_PROP);
        if (deletedRowsStr == null) {
            // TODO Iceberg should guarantee this is always present (https://github.com/apache/iceberg/issues/4647)
            return OptionalLong.empty();
        }
        long deletedRecords = Long.parseLong(deletedRowsStr);
        long removedPositionDeletes = Long.parseLong(summary.getOrDefault(REMOVED_POS_DELETES_PROP, "0"));
        long removedEqualityDeletes = Long.parseLong(summary.getOrDefault(REMOVED_EQ_DELETES_PROP, "0"));
        return OptionalLong.of(deletedRecords - removedPositionDeletes - removedEqualityDeletes);
    }

    @Override
    public void truncateTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle table = checkValidTableHandle(tableHandle);
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        DeleteFiles deleteFiles = icebergTable.newDelete()
                .deleteFromRowFilter(alwaysTrue())
                .scanManifestsWith(icebergScanExecutor);
        commitUpdate(deleteFiles, session, "truncate");
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;

        if (table.getLimit().isPresent() && table.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        if (!table.getUnenforcedPredicate().isAll()) {
            return Optional.empty();
        }

        table = new IcebergTableHandle(
                table.getSchemaName(),
                table.getTableName(),
                table.getTableType(),
                table.getSnapshotId(),
                table.getTableSchemaJson(),
                table.getPartitionSpecJson(),
                table.getFormatVersion(),
                table.getUnenforcedPredicate(), // known to be ALL
                table.getEnforcedPredicate(),
                OptionalLong.of(limit),
                table.getProjectedColumns(),
                table.getNameMappingJson(),
                table.getTableLocation(),
                table.getStorageProperties(),
                table.getTablePartitioning(),
                table.isRecordScannedFiles(),
                table.getMaxScannedFileSize(),
                table.getConstraintColumns(),
                table.getForAnalyze(),
                table.getUpdateKind());

        return Optional.of(new LimitApplicationResult<>(table, false, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        IcebergTableHandle table = (IcebergTableHandle) handle;
        UtcConstraintExtractor.ExtractionResult extractionResult = extractTupleDomain(constraint);
        TupleDomain<IcebergColumnHandle> predicate = extractionResult.tupleDomain()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (predicate.isAll() && constraint.getPredicateColumns().isEmpty()) {
            return Optional.empty();
        }
        if (table.getLimit().isPresent()) {
            // TODO we probably can allow predicate pushdown after we accepted limit. Currently, this is theoretical because we don't enforce limit, so
            //  LimitNode remains above TableScan, and there is no "push filter through limit" optimization.
            return Optional.empty();
        }

        TupleDomain<IcebergColumnHandle> newEnforcedConstraint;
        TupleDomain<IcebergColumnHandle> newUnenforcedConstraint;
        TupleDomain<IcebergColumnHandle> remainingConstraint;
        if (predicate.isNone()) {
            // Engine does not pass none Constraint.summary. It can become none when combined with the expression and connector's domain knowledge.
            newEnforcedConstraint = TupleDomain.none();
            newUnenforcedConstraint = TupleDomain.all();
            remainingConstraint = TupleDomain.all();
        }
        else {
            Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());

            Set<Integer> partitionSpecIds = table.getSnapshotId().map(
                            snapshot -> loadAllManifestsFromSnapshot(icebergTable, icebergTable.snapshot(snapshot)).stream()
                                    .map(ManifestFile::partitionSpecId)
                                    .collect(toImmutableSet()))
                    // No snapshot, so no data. This case doesn't matter.
                    .orElseGet(() -> ImmutableSet.copyOf(icebergTable.specs().keySet()));

            Map<IcebergColumnHandle, Domain> unsupported = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newEnforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> newUnenforced = new LinkedHashMap<>();
            Map<IcebergColumnHandle, Domain> domains = predicate.getDomains().orElseThrow(() -> new VerifyException("No domains"));
            domains.forEach((columnHandle, domain) -> {
                if (!isConvertibleToIcebergExpression(domain)) {
                    unsupported.put(columnHandle, domain);
                }
                else if (canEnforceColumnConstraintInSpecs(typeManager.getTypeOperators(), icebergTable, partitionSpecIds, columnHandle, domain)) {
                    newEnforced.put(columnHandle, domain);
                }
                else if (isMetadataColumnId(columnHandle.getId())) {
                    if (columnHandle.isPartitionColumn() || columnHandle.isPathColumn() || columnHandle.isFileModifiedTimeColumn()) {
                        newEnforced.put(columnHandle, domain);
                    }
                    else {
                        unsupported.put(columnHandle, domain);
                    }
                }
                else {
                    newUnenforced.put(columnHandle, domain);
                }
            });

            newEnforcedConstraint = TupleDomain.withColumnDomains(newEnforced).intersect(table.getEnforcedPredicate());
            newUnenforcedConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(table.getUnenforcedPredicate());
            remainingConstraint = TupleDomain.withColumnDomains(newUnenforced).intersect(TupleDomain.withColumnDomains(unsupported));
        }

        Set<IcebergColumnHandle> newConstraintColumns = Streams.concat(
                        table.getConstraintColumns().stream(),
                        constraint.getPredicateColumns().orElseGet(ImmutableSet::of).stream()
                                .map(columnHandle -> (IcebergColumnHandle) columnHandle))
                .collect(toImmutableSet());

        if (newEnforcedConstraint.equals(table.getEnforcedPredicate())
                && newUnenforcedConstraint.equals(table.getUnenforcedPredicate())
                && newConstraintColumns.equals(table.getConstraintColumns())) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                new IcebergTableHandle(
                        table.getSchemaName(),
                        table.getTableName(),
                        table.getTableType(),
                        table.getSnapshotId(),
                        table.getTableSchemaJson(),
                        table.getPartitionSpecJson(),
                        table.getFormatVersion(),
                        newUnenforcedConstraint,
                        newEnforcedConstraint,
                        table.getLimit(),
                        table.getProjectedColumns(),
                        table.getNameMappingJson(),
                        table.getTableLocation(),
                        table.getStorageProperties(),
                        table.getTablePartitioning(),
                        table.isRecordScannedFiles(),
                        table.getMaxScannedFileSize(),
                        newConstraintColumns,
                        table.getForAnalyze(),
                        table.getUpdateKind()),
                remainingConstraint.transformKeys(ColumnHandle.class::cast),
                extractionResult.remainingExpression(),
                false));
    }

    private static List<ManifestFile> loadAllManifestsFromSnapshot(Table icebergTable, Snapshot snapshot)
    {
        if (snapshot == null) {
            return ImmutableList.of();
        }
        try {
            return snapshot.allManifests(icebergTable.io());
        }
        catch (NotFoundException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Error accessing manifest file for table %s".formatted(icebergTable.name()), e);
        }
    }

    /**
     * Use instead of loadAllManifestsFromSnapshot when loading manifests from multiple distinct snapshots
     * Each BaseSnapshot object caches manifest files separately, so loading manifests from multiple distinct snapshots
     * results in O(num_snapshots^2) copies of the same manifest file metadata in memory
     */
    private static List<ManifestFile> loadAllManifestsFromManifestList(Table icebergTable, String manifestListLocation)
    {
        try {
            return IcebergManifestUtils.read(icebergTable.io(), manifestListLocation);
        }
        catch (NotFoundException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, "Error accessing manifest file for table %s".formatted(icebergTable.name()), e);
        }
    }

    private static Set<Integer> identityPartitionColumnsInAllSpecs(Table table)
    {
        // Extract identity partition column source ids common to ALL specs
        return table.spec().fields().stream()
                .filter(field -> field.transform().isIdentity())
                .filter(field -> table.specs().values().stream().allMatch(spec -> spec.fields().contains(field)))
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }

        // Create projected column representations for supported sub expressions. Simple column references and chain of
        // dereferences on a variable are supported right now.
        Set<ConnectorExpression> projectedExpressions = projections.stream()
                .flatMap(expression -> extractSupportedProjectedColumns(expression).stream())
                .collect(toImmutableSet());

        Map<ConnectorExpression, ProjectedColumnRepresentation> columnProjections = projectedExpressions.stream()
                .collect(toImmutableMap(identity(), ApplyProjectionUtil::createProjectedColumnRepresentation));

        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) handle;

        // all references are simple variables
        if (columnProjections.values().stream().allMatch(ProjectedColumnRepresentation::isVariable)) {
            Set<IcebergColumnHandle> projectedColumns = assignments.values().stream()
                    .map(IcebergColumnHandle.class::cast)
                    .collect(toImmutableSet());
            if (icebergTableHandle.getProjectedColumns().equals(projectedColumns)) {
                return Optional.empty();
            }
            List<Assignment> assignmentsList = assignments.entrySet().stream()
                    .map(assignment -> new Assignment(
                            assignment.getKey(),
                            assignment.getValue(),
                            ((IcebergColumnHandle) assignment.getValue()).getType()))
                    .collect(toImmutableList());

            return Optional.of(new ProjectionApplicationResult<>(
                    icebergTableHandle.withProjectedColumns(projectedColumns),
                    projections,
                    assignmentsList,
                    false));
        }

        Map<String, Assignment> newAssignments = new HashMap<>();
        ImmutableMap.Builder<ConnectorExpression, Variable> newVariablesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<IcebergColumnHandle> projectedColumnsBuilder = ImmutableSet.builder();

        for (Map.Entry<ConnectorExpression, ProjectedColumnRepresentation> entry : columnProjections.entrySet()) {
            ConnectorExpression expression = entry.getKey();
            ProjectedColumnRepresentation projectedColumn = entry.getValue();

            IcebergColumnHandle baseColumnHandle = (IcebergColumnHandle) assignments.get(projectedColumn.getVariable().getName());
            IcebergColumnHandle projectedColumnHandle = createProjectedColumnHandle(baseColumnHandle, projectedColumn.getDereferenceIndices(), expression.getType());
            String projectedColumnName = projectedColumnHandle.getQualifiedName();

            Variable projectedColumnVariable = new Variable(projectedColumnName, expression.getType());
            Assignment newAssignment = new Assignment(projectedColumnName, projectedColumnHandle, expression.getType());
            newAssignments.putIfAbsent(projectedColumnName, newAssignment);

            newVariablesBuilder.put(expression, projectedColumnVariable);
            projectedColumnsBuilder.add(projectedColumnHandle);
        }

        // Modify projections to refer to new variables
        Map<ConnectorExpression, Variable> newVariables = newVariablesBuilder.buildOrThrow();
        List<ConnectorExpression> newProjections = projections.stream()
                .map(expression -> replaceWithNewVariables(expression, newVariables))
                .collect(toImmutableList());

        List<Assignment> outputAssignments = ImmutableList.copyOf(newAssignments.values());
        return Optional.of(new ProjectionApplicationResult<>(
                icebergTableHandle.withProjectedColumns(projectedColumnsBuilder.build()),
                newProjections,
                outputAssignments,
                false));
    }

    private static IcebergColumnHandle createProjectedColumnHandle(IcebergColumnHandle column, List<Integer> indices, io.trino.spi.type.Type projectedColumnType)
    {
        if (indices.isEmpty()) {
            return column;
        }
        ImmutableList.Builder<Integer> fullPath = ImmutableList.builder();
        fullPath.addAll(column.getPath());

        ColumnIdentity projectedColumnIdentity = column.getColumnIdentity();
        for (int index : indices) {
            // Position based lookup, not FieldId based
            projectedColumnIdentity = projectedColumnIdentity.getChildren().get(index);
            fullPath.add(projectedColumnIdentity.getId());
        }

        return IcebergColumnHandle.optional(column.getBaseColumnIdentity())
                .fieldType(column.getBaseType(), projectedColumnType)
                .path(fullPath.build())
                .build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }

        IcebergTableHandle originalHandle = (IcebergTableHandle) tableHandle;

        if (originalHandle.isRecordScannedFiles()) {
            return TableStatistics.empty();
        }
        // Certain table handle attributes are not applicable to select queries (which need stats).
        // If this changes, the caching logic may here may need to be revised.
        checkArgument(originalHandle.getMaxScannedFileSize().isEmpty(), "Unexpected max scanned file size set");

        IcebergTableHandle cacheKey = new IcebergTableHandle(
                originalHandle.getSchemaName(),
                originalHandle.getTableName(),
                originalHandle.getTableType(),
                originalHandle.getSnapshotId(),
                originalHandle.getTableSchemaJson(),
                originalHandle.getPartitionSpecJson(),
                originalHandle.getFormatVersion(),
                originalHandle.getUnenforcedPredicate(),
                originalHandle.getEnforcedPredicate(),
                OptionalLong.empty(), // limit is currently not included in stats and is not enforced by the connector
                ImmutableSet.of(), // projectedColumns are used to request statistics only for the required columns, but are not part of cache key
                originalHandle.getNameMappingJson(),
                originalHandle.getTableLocation(),
                originalHandle.getStorageProperties(),
                Optional.empty(), // requiredTablePartitioning does not affect stats
                false, // recordScannedFiles does not affect stats
                originalHandle.getMaxScannedFileSize(),
                ImmutableSet.of(), // constraintColumns do not affect stats
                Optional.empty(), // forAnalyze does not affect stats
                originalHandle.getUpdateKind());
        return getIncrementally(
                tableStatisticsCache,
                cacheKey,
                currentStatistics -> currentStatistics.getColumnStatistics().keySet().containsAll(originalHandle.getProjectedColumns()),
                projectedColumns -> {
                    Table icebergTable = catalog.loadTable(session, originalHandle.getSchemaTableName());
                    return tableStatisticsReader.getTableStatistics(
                            session,
                            originalHandle,
                            projectedColumns,
                            icebergTable);
                },
                originalHandle.getProjectedColumns());
    }

    @Override
    public void setTableAuthorization(ConnectorSession session, SchemaTableName tableName, TrinoPrincipal principal)
    {
        catalog.setTablePrincipal(session, tableName, principal);
    }

    private Optional<Long> getCurrentSnapshotId(Table table)
    {
        return Optional.ofNullable(table.currentSnapshot()).map(Snapshot::snapshotId);
    }

    Table getIcebergTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return catalog.loadTable(session, schemaTableName);
    }

    @Override
    public void createMaterializedView(
            ConnectorSession session,
            SchemaTableName viewName,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> properties,
            boolean replace,
            boolean ignoreExisting)
    {
        catalog.createMaterializedView(session, viewName, definition, properties, replace, ignoreExisting);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        catalog.dropMaterializedView(session, viewName);
    }

    @Override
    public boolean delegateMaterializedViewRefreshToConnector(ConnectorSession session, SchemaTableName viewName)
    {
        return false;
    }

    @Override
    public ConnectorInsertTableHandle beginRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            boolean hasForeignSourceTables,
            RetryMode retryMode,
            RefreshType refreshType)
    {
        checkState(fromSnapshotForRefresh.isEmpty(), "From Snapshot must be empty at the start of MV refresh operation.");
        IcebergTableHandle table = (IcebergTableHandle) tableHandle;
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        beginTransaction(icebergTable);

        Optional<String> dependencies = Optional.ofNullable(icebergTable.currentSnapshot())
                .map(Snapshot::summary)
                .map(summary -> summary.get(DEPENDS_ON_TABLES));

        boolean shouldUseIncremental = isIncrementalRefreshEnabled(session)
                && refreshType == RefreshType.INCREMENTAL
                // there is a single source table
                && sourceTableHandles.size() == 1
                // and there are no other foreign sources
                && !hasForeignSourceTables
                // and the source table's fromSnapshot is available in the MV snapshot summary
                && dependencies.isPresent() && !dependencies.get().equals(UNKNOWN_SNAPSHOT_TOKEN);

        if (shouldUseIncremental) {
            Map<String, String> sourceTableToSnapshot = MAP_SPLITTER.split(dependencies.get());
            checkState(sourceTableToSnapshot.size() == 1, "Expected %s to contain only single source table in snapshot summary", sourceTableToSnapshot);
            Map.Entry<String, String> sourceTable = getOnlyElement(sourceTableToSnapshot.entrySet());
            String[] schemaTable = sourceTable.getKey().split("\\.");
            IcebergTableHandle handle = (IcebergTableHandle) getOnlyElement(sourceTableHandles);
            SchemaTableName sourceSchemaTable = new SchemaTableName(schemaTable[0], schemaTable[1]);
            checkState(sourceSchemaTable.equals(handle.getSchemaTableName()), "Source table name %s doesn't match handle table name %s", sourceSchemaTable, handle.getSchemaTableName());
            fromSnapshotForRefresh = Optional.of(Long.parseLong(sourceTable.getValue()));
        }

        return newWritableTableHandle(table.getSchemaTableName(), icebergTable);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishRefreshMaterializedView(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<ConnectorTableHandle> sourceTableHandles,
            boolean hasForeignSourceTables,
            boolean hasSourceTableFunctions)
    {
        IcebergWritableTableHandle table = (IcebergWritableTableHandle) insertHandle;

        Table icebergTable = transaction.table();
        boolean isFullRefresh = fromSnapshotForRefresh.isEmpty();
        if (isFullRefresh) {
            // delete before insert .. simulating overwrite
            log.info("Performing full MV refresh for storage table: %s", table.name());
            transaction.newDelete()
                    .deleteFromRowFilter(Expressions.alwaysTrue())
                    .scanManifestsWith(icebergScanExecutor)
                    .commit();
        }
        else {
            log.info("Performing incremental MV refresh for storage table: %s", table.name());
        }

        List<CommitTaskData> commitTasks = fragments.stream()
                .map(Slice::getInput)
                .map(commitTaskCodec::fromJson)
                .collect(toImmutableList());

        Type[] partitionColumnTypes = icebergTable.spec().fields().stream()
                .map(field -> field.transform().getResultType(
                        icebergTable.schema().findType(field.sourceId())))
                .toArray(Type[]::new);

        AppendFiles appendFiles = isMergeManifestsOnWrite(session) ? transaction.newAppend() : transaction.newFastAppend();
        for (CommitTaskData task : commitTasks) {
            DataFiles.Builder builder = DataFiles.builder(icebergTable.spec())
                    .withPath(task.path())
                    .withFileSizeInBytes(task.fileSizeInBytes())
                    .withFormat(table.fileFormat().toIceberg())
                    .withMetrics(task.metrics().metrics());

            if (!icebergTable.spec().fields().isEmpty()) {
                String partitionDataJson = task.partitionDataJson()
                        .orElseThrow(() -> new VerifyException("No partition data for partitioned table"));
                builder.withPartition(PartitionData.fromJson(partitionDataJson, partitionColumnTypes));
            }

            appendFiles.appendFile(builder.build());
        }

        List<String> tableDependencies = new ArrayList<>();
        sourceTableHandles.stream()
                .map(IcebergTableHandle.class::cast)
                .map(handle -> "%s=%s".formatted(
                        handle.getSchemaTableName(),
                        handle.getSnapshotId().map(Object::toString).orElse("")))
                .forEach(tableDependencies::add);
        if (hasForeignSourceTables) {
            tableDependencies.add(UNKNOWN_SNAPSHOT_TOKEN);
        }

        // Update the 'dependsOnTables' property that tracks tables on which the materialized view depends and the corresponding snapshot ids of the tables
        appendFiles.set(DEPENDS_ON_TABLES, String.join(",", tableDependencies));
        appendFiles.set(DEPENDS_ON_TABLE_FUNCTIONS, String.valueOf(hasSourceTableFunctions));
        appendFiles.set(TRINO_QUERY_START_TIME, session.getStart().toString());
        appendFiles.scanManifestsWith(icebergScanExecutor);
        commitUpdateAndTransaction(appendFiles, session, transaction, "refresh materialized view");
        transaction = null;
        fromSnapshotForRefresh = Optional.empty();
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream()
                .map(CommitTaskData::path)
                .collect(toImmutableList())));
    }

    @Override
    public List<SchemaTableName> listMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        return catalog.listTables(session, schemaName).stream()
                .filter(info -> info.extendedRelationType() == TableInfo.ExtendedRelationType.TRINO_MATERIALIZED_VIEW)
                .map(TableInfo::tableName)
                .toList();
    }

    @Override
    public Map<SchemaTableName, ConnectorMaterializedViewDefinition> getMaterializedViews(ConnectorSession session, Optional<String> schemaName)
    {
        Map<SchemaTableName, ConnectorMaterializedViewDefinition> materializedViews = new HashMap<>();
        for (SchemaTableName name : listMaterializedViews(session, schemaName)) {
            try {
                getMaterializedView(session, name).ifPresent(view -> materializedViews.put(name, view));
            }
            catch (RuntimeException e) {
                // Materialized view can be being removed and this may cause all sorts of exceptions. Log, because we're catching broadly.
                log.warn(e, "Failed to access metadata of materialized view %s during listing", name);
            }
        }
        return materializedViews;
    }

    @Override
    public Optional<ConnectorMaterializedViewDefinition> getMaterializedView(ConnectorSession session, SchemaTableName viewName)
    {
        return catalog.getMaterializedView(session, viewName);
    }

    @Override
    public Map<String, Object> getMaterializedViewProperties(ConnectorSession session, SchemaTableName viewName, ConnectorMaterializedViewDefinition definition)
    {
        return catalog.getMaterializedViewProperties(session, viewName, definition);
    }

    @Override
    public void renameMaterializedView(ConnectorSession session, SchemaTableName source, SchemaTableName target)
    {
        // TODO (https://github.com/trinodb/trino/issues/9594) support rename across schemas
        if (!source.getSchemaName().equals(target.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "Materialized View rename across schemas is not supported");
        }
        catalog.renameMaterializedView(session, source, target);
    }

    @Override
    public MaterializedViewFreshness getMaterializedViewFreshness(ConnectorSession session, SchemaTableName materializedViewName)
    {
        Optional<ConnectorMaterializedViewDefinition> materializedViewDefinition = getMaterializedView(session, materializedViewName);
        if (materializedViewDefinition.isEmpty()) {
            // View not found, might have been concurrently deleted
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        SchemaTableName storageTableName = materializedViewDefinition.get().getStorageTable()
                .map(CatalogSchemaTableName::getSchemaTableName)
                .orElseThrow(() -> new IllegalStateException("Storage table missing in definition of materialized view " + materializedViewName));

        Table icebergTable = catalog.loadTable(session, storageTableName);
        Optional<Snapshot> currentSnapshot = Optional.ofNullable(icebergTable.currentSnapshot());
        String dependsOnTables = currentSnapshot
                .map(snapshot -> snapshot.summary().getOrDefault(DEPENDS_ON_TABLES, ""))
                .orElse("");
        boolean dependsOnTableFunctions = currentSnapshot
                .map(snapshot -> Boolean.valueOf(snapshot.summary().getOrDefault(DEPENDS_ON_TABLE_FUNCTIONS, "false")))
                .orElse(false);

        Optional<Instant> refreshTime = currentSnapshot.map(snapshot -> snapshot.summary().get(TRINO_QUERY_START_TIME))
                .map(Instant::parse)
                .or(() -> currentSnapshot.map(snapshot -> Instant.ofEpochMilli(snapshot.timestampMillis())));

        if (dependsOnTableFunctions) {
            // It can't be determined whether a value returned by table function is STALE or not
            return new MaterializedViewFreshness(UNKNOWN, refreshTime);
        }

        if (dependsOnTables.isEmpty()) {
            // Information missing. While it's "unknown" whether storage is stale, we return "stale".
            // Normally dependsOnTables may be missing only when there was no refresh yet.
            return new MaterializedViewFreshness(STALE, Optional.empty());
        }

        boolean hasUnknownTables = false;
        Optional<Long> firstTableChange = Optional.of(Long.MAX_VALUE);
        ImmutableList.Builder<Callable<TableChangeInfo>> tableChangeInfoTasks = ImmutableList.builder();
        for (String tableToSnapShot : Splitter.on(',').split(dependsOnTables)) {
            if (tableToSnapShot.equals(UNKNOWN_SNAPSHOT_TOKEN)) {
                hasUnknownTables = true;
                firstTableChange = Optional.empty();
                continue;
            }

            tableChangeInfoTasks.add(() -> getTableChangeInfo(session, tableToSnapShot));
        }

        boolean hasStaleIcebergTables = false;
        List<TableChangeInfo> tableChangeInfos;

        try {
            tableChangeInfos = processWithAdditionalThreads(tableChangeInfoTasks.build(), metadataFetchingExecutor);
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }

        verifyNotNull(tableChangeInfos);

        for (TableChangeInfo tableChangeInfo : tableChangeInfos) {
            switch (tableChangeInfo) {
                case NoTableChange() -> {
                    // Fresh
                }
                case FirstChangeSnapshot(Snapshot snapshot) -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = firstTableChange
                            .map(epochMilli -> Math.min(epochMilli, snapshot.timestampMillis()));
                }
                case UnknownTableChange(), GoneOrCorruptedTableChange() -> {
                    hasStaleIcebergTables = true;
                    firstTableChange = Optional.empty();
                }
            }
        }

        Optional<Instant> lastFreshTime = firstTableChange
                .map(Instant::ofEpochMilli)
                .or(() -> refreshTime);
        if (hasStaleIcebergTables) {
            return new MaterializedViewFreshness(STALE, lastFreshTime);
        }
        if (hasUnknownTables) {
            return new MaterializedViewFreshness(UNKNOWN, lastFreshTime);
        }
        return new MaterializedViewFreshness(FRESH, Optional.empty());
    }

    private TableChangeInfo getTableChangeInfo(ConnectorSession session, String entry)
    {
        List<String> keyValue = Splitter.on("=").splitToList(entry);
        if (keyValue.size() != 2) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid entry in '%s' property: %s'", DEPENDS_ON_TABLES, entry));
        }
        String tableName = keyValue.get(0);
        String value = keyValue.get(1);
        List<String> strings = Splitter.on(".").splitToList(tableName);
        if (strings.size() == 3) {
            strings = strings.subList(1, 3);
        }
        else if (strings.size() != 2) {
            throw new TrinoException(ICEBERG_INVALID_METADATA, format("Invalid table name in '%s' property: %s'", DEPENDS_ON_TABLES, strings));
        }
        String schema = strings.get(0);
        String name = strings.get(1);
        SchemaTableName schemaTableName = new SchemaTableName(schema, name);
        ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName, Optional.empty(), Optional.empty());

        if (tableHandle == null || tableHandle instanceof CorruptedIcebergTableHandle) {
            // Base table is gone or table is corrupted
            return new GoneOrCorruptedTableChange();
        }
        Optional<Long> snapshotAtRefresh;
        if (value.isEmpty()) {
            snapshotAtRefresh = Optional.empty();
        }
        else {
            snapshotAtRefresh = Optional.of(Long.parseLong(value));
        }
        return getTableChangeInfo(session, (IcebergTableHandle) tableHandle, snapshotAtRefresh);
    }

    private TableChangeInfo getTableChangeInfo(ConnectorSession session, IcebergTableHandle table, Optional<Long> snapshotAtRefresh)
    {
        Table icebergTable = catalog.loadTable(session, table.getSchemaTableName());
        Snapshot currentSnapshot = icebergTable.currentSnapshot();

        if (snapshotAtRefresh.isEmpty()) {
            // Table had no snapshot at refresh time.
            if (currentSnapshot == null) {
                return new NoTableChange();
            }
            return firstSnapshot(icebergTable)
                    .<TableChangeInfo>map(FirstChangeSnapshot::new)
                    .orElse(new UnknownTableChange());
        }

        if (snapshotAtRefresh.get() == currentSnapshot.snapshotId()) {
            return new NoTableChange();
        }
        return firstSnapshotAfter(icebergTable, snapshotAtRefresh.get())
                .<TableChangeInfo>map(FirstChangeSnapshot::new)
                .orElse(new UnknownTableChange());
    }

    @Override
    public void setColumnComment(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column, Optional<String> comment)
    {
        catalog.updateColumnComment(session, ((IcebergTableHandle) tableHandle).getSchemaTableName(), ((IcebergColumnHandle) column).getColumnIdentity(), comment);
    }

    @Override
    public Optional<CatalogSchemaTableName> redirectTable(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<String> targetCatalogName = getHiveCatalogName(session);
        if (targetCatalogName.isEmpty()) {
            return Optional.empty();
        }
        return catalog.redirectTable(session, tableName, targetCatalogName.get());
    }

    @Override
    public boolean allowSplittingReadIntoMultipleSubQueries(ConnectorSession session, ConnectorTableHandle connectorTableHandle)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) connectorTableHandle;
        IcebergFileFormat storageFormat = getFileFormat(tableHandle.getStorageProperties());

        return storageFormat == ORC || storageFormat == PARQUET;
    }

    @Override
    public WriterScalingOptions getNewTableWriterScalingOptions(ConnectorSession session, SchemaTableName tableName, Map<String, Object> tableProperties)
    {
        return WriterScalingOptions.ENABLED;
    }

    @Override
    public WriterScalingOptions getInsertWriterScalingOptions(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return WriterScalingOptions.ENABLED;
    }

    public Optional<Long> getIncrementalRefreshFromSnapshot()
    {
        return fromSnapshotForRefresh;
    }

    public void disableIncrementalRefresh()
    {
        fromSnapshotForRefresh = Optional.empty();
    }

    private static CollectedStatistics processComputedTableStatistics(Table table, Collection<ComputedStatistics> computedStatistics)
    {
        Map<String, Integer> columnNameToId = table.schema().columns().stream()
                .collect(toImmutableMap(nestedField -> nestedField.name().toLowerCase(ENGLISH), Types.NestedField::fieldId));

        ImmutableMap.Builder<Integer, CompactSketch> ndvSketches = ImmutableMap.builder();
        for (ComputedStatistics computedStatistic : computedStatistics) {
            verify(computedStatistic.getGroupingColumns().isEmpty() && computedStatistic.getGroupingValues().isEmpty(), "Unexpected grouping");
            verify(computedStatistic.getTableStatistics().isEmpty(), "Unexpected table statistics");
            for (Map.Entry<ColumnStatisticMetadata, Block> entry : computedStatistic.getColumnStatistics().entrySet()) {
                ColumnStatisticMetadata statisticMetadata = entry.getKey();
                if (statisticMetadata.getConnectorAggregationId().equals(NUMBER_OF_DISTINCT_VALUES_NAME)) {
                    Integer columnId = verifyNotNull(
                            columnNameToId.get(statisticMetadata.getColumnName()),
                            "Column not found in table: [%s]",
                            statisticMetadata.getColumnName());
                    CompactSketch sketch = DataSketchStateSerializer.deserialize(entry.getValue(), 0);
                    ndvSketches.put(columnId, sketch);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported statistic: " + statisticMetadata);
                }
            }
        }

        return new CollectedStatistics(ndvSketches.buildOrThrow());
    }

    private void beginTransaction(Table icebergTable)
    {
        verify(transaction == null, "transaction already set");
        transaction = catalog.newTransaction(icebergTable);
    }

    private static IcebergTableHandle checkValidTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        if (tableHandle instanceof CorruptedIcebergTableHandle corruptedTableHandle) {
            throw corruptedTableHandle.createException();
        }
        return ((IcebergTableHandle) tableHandle);
    }

    private sealed interface TableChangeInfo
            permits NoTableChange, FirstChangeSnapshot, UnknownTableChange, GoneOrCorruptedTableChange {}

    private record NoTableChange()
            implements TableChangeInfo {}

    private record FirstChangeSnapshot(Snapshot snapshot)
            implements TableChangeInfo
    {
        FirstChangeSnapshot
        {
            requireNonNull(snapshot, "snapshot is null");
        }
    }

    private record UnknownTableChange()
            implements TableChangeInfo {}

    private record GoneOrCorruptedTableChange()
            implements TableChangeInfo {}

    private static TableStatistics getIncrementally(
            Map<IcebergTableHandle, AtomicReference<TableStatistics>> cache,
            IcebergTableHandle key,
            Predicate<TableStatistics> isSufficient,
            Function<Set<IcebergColumnHandle>, TableStatistics> columnStatisticsLoader,
            Set<IcebergColumnHandle> projectedColumns)
    {
        AtomicReference<TableStatistics> valueHolder = cache.computeIfAbsent(key, _ -> new AtomicReference<>());
        TableStatistics oldValue = valueHolder.get();
        if (oldValue != null && isSufficient.test(oldValue)) {
            return oldValue;
        }

        TableStatistics newValue;
        if (oldValue == null) {
            newValue = columnStatisticsLoader.apply(projectedColumns);
        }
        else {
            Sets.SetView<IcebergColumnHandle> missingColumns = difference(projectedColumns, oldValue.getColumnStatistics().keySet());
            newValue = columnStatisticsLoader.apply(missingColumns);
        }

        verifyNotNull(newValue, "loader returned null for %s", key);

        TableStatistics merged = mergeColumnStatistics(oldValue, newValue);
        if (!valueHolder.compareAndSet(oldValue, merged)) {
            // if the value changed in the valueHolder, we only add newly loaded value to be sure we have up-to-date value
            valueHolder.accumulateAndGet(newValue, IcebergMetadata::mergeColumnStatistics);
        }
        return merged;
    }

    private static TableStatistics mergeColumnStatistics(TableStatistics currentStats, TableStatistics newStats)
    {
        requireNonNull(newStats, "newStats is null");
        TableStatistics.Builder statisticsBuilder = TableStatistics.builder();
        if (currentStats != null) {
            currentStats.getColumnStatistics().forEach(statisticsBuilder::setColumnStatistics);
        }
        statisticsBuilder.setRowCount(newStats.getRowCount());
        newStats.getColumnStatistics().forEach(statisticsBuilder::setColumnStatistics);
        return statisticsBuilder.build();
    }
}
