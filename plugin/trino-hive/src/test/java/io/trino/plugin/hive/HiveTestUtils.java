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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import io.trino.hdfs.gcs.GoogleGcsConfigurationInitializer;
import io.trino.hdfs.gcs.HiveGcsConfig;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.hive.line.CsvFileWriterFactory;
import io.trino.plugin.hive.line.CsvPageSourceFactory;
import io.trino.plugin.hive.line.JsonFileWriterFactory;
import io.trino.plugin.hive.line.JsonPageSourceFactory;
import io.trino.plugin.hive.line.OpenXJsonFileWriterFactory;
import io.trino.plugin.hive.line.OpenXJsonPageSourceFactory;
import io.trino.plugin.hive.line.RegexFileWriterFactory;
import io.trino.plugin.hive.line.RegexPageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleSequenceFileWriterFactory;
import io.trino.plugin.hive.line.SimpleTextFilePageSourceFactory;
import io.trino.plugin.hive.line.SimpleTextFileWriterFactory;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.plugin.hive.s3select.S3SelectRecordCursorProvider;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.hive.common.type.Date;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.util.StructuralTestUtil.appendToBlockBuilder;

public final class HiveTestUtils
{
    private HiveTestUtils() {}

    public static final ConnectorSession SESSION = getHiveSession(new HiveConfig());

    public static final Optional<HostAndPort> SOCKS_PROXY = Optional.ofNullable(System.getProperty("hive.metastore.thrift.client.socks-proxy"))
            .map(HostAndPort::fromString);

    public static final DynamicHdfsConfiguration HDFS_CONFIGURATION = new DynamicHdfsConfiguration(
            new HdfsConfigurationInitializer(
                    new HdfsConfig()
                            .setSocksProxy(SOCKS_PROXY.orElse(null)),
                    ImmutableSet.of(
                            new TrinoS3ConfigurationInitializer(new HiveS3Config()),
                            new GoogleGcsConfigurationInitializer(new HiveGcsConfig()),
                            new TrinoAzureConfigurationInitializer(new HiveAzureConfig()))),
            ImmutableSet.of());

    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment(
            HDFS_CONFIGURATION,
            new HdfsConfig(),
            new NoHdfsAuthentication());

    public static final TrinoHdfsFileSystemStats HDFS_FILE_SYSTEM_STATS = new TrinoHdfsFileSystemStats();
    public static final HdfsFileSystemFactory HDFS_FILE_SYSTEM_FACTORY = new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS);

    public static final PageSorter PAGE_SORTER = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    public static ConnectorSession getHiveSession(HiveConfig hiveConfig)
    {
        return getHiveSession(hiveConfig, new OrcReaderConfig());
    }

    public static TestingConnectorSession getHiveSession(HiveConfig hiveConfig, OrcReaderConfig orcReaderConfig)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(getHiveSessionProperties(hiveConfig, orcReaderConfig).getSessionProperties())
                .build();
    }

    public static TestingConnectorSession getHiveSession(HiveConfig hiveConfig, ParquetWriterConfig parquetWriterConfig)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(getHiveSessionProperties(hiveConfig, parquetWriterConfig).getSessionProperties())
                .build();
    }

    public static TestingConnectorSession getHiveSession(HiveConfig hiveConfig, ParquetReaderConfig parquetReaderConfig)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(getHiveSessionProperties(hiveConfig, parquetReaderConfig).getSessionProperties())
                .build();
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig)
    {
        return getHiveSessionProperties(hiveConfig, new OrcReaderConfig());
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig, OrcReaderConfig orcReaderConfig)
    {
        return new HiveSessionProperties(
                hiveConfig,
                new HiveFormatsConfig(),
                orcReaderConfig,
                new OrcWriterConfig(),
                new ParquetReaderConfig(),
                new ParquetWriterConfig());
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig, ParquetWriterConfig parquetWriterConfig)
    {
        return new HiveSessionProperties(
                hiveConfig,
                new HiveFormatsConfig(),
                new OrcReaderConfig(),
                new OrcWriterConfig(),
                new ParquetReaderConfig(),
                parquetWriterConfig);
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig, ParquetReaderConfig parquetReaderConfig)
    {
        return new HiveSessionProperties(
                hiveConfig,
                new HiveFormatsConfig(),
                new OrcReaderConfig(),
                new OrcWriterConfig(),
                parquetReaderConfig,
                new ParquetWriterConfig());
    }

    public static Set<HivePageSourceFactory> getDefaultHivePageSourceFactories(HdfsEnvironment hdfsEnvironment, HiveConfig hiveConfig)
    {
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS);
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        return ImmutableSet.<HivePageSourceFactory>builder()
                .add(new CsvPageSourceFactory(fileSystemFactory, stats, hiveConfig))
                .add(new JsonPageSourceFactory(fileSystemFactory, stats, hiveConfig))
                .add(new OpenXJsonPageSourceFactory(fileSystemFactory, stats, hiveConfig))
                .add(new RegexPageSourceFactory(fileSystemFactory, stats, hiveConfig))
                .add(new SimpleTextFilePageSourceFactory(fileSystemFactory, stats, hiveConfig))
                .add(new SimpleSequenceFilePageSourceFactory(fileSystemFactory, stats, hiveConfig))
                .add(new RcFilePageSourceFactory(TESTING_TYPE_MANAGER, fileSystemFactory, stats, hiveConfig))
                .add(new OrcPageSourceFactory(new OrcReaderConfig(), fileSystemFactory, stats, hiveConfig))
                .add(new ParquetPageSourceFactory(fileSystemFactory, stats, new ParquetReaderConfig(), hiveConfig))
                .build();
    }

    public static Set<HiveRecordCursorProvider> getDefaultHiveRecordCursorProviders(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        return ImmutableSet.of(new S3SelectRecordCursorProvider(hdfsEnvironment, new TrinoS3ClientFactory(hiveConfig)));
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        TrinoFileSystemFactory fileSystemFactory = new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS);
        NodeVersion nodeVersion = new NodeVersion("test_version");
        return ImmutableSet.<HiveFileWriterFactory>builder()
                .add(new CsvFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new JsonFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new RegexFileWriterFactory())
                .add(new OpenXJsonFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new SimpleTextFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new SimpleSequenceFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER, nodeVersion))
                .add(new RcFileFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER, nodeVersion, hiveConfig))
                .add(getDefaultOrcFileWriterFactory(hdfsEnvironment))
                .build();
    }

    private static OrcFileWriterFactory getDefaultOrcFileWriterFactory(HdfsEnvironment hdfsEnvironment)
    {
        return new OrcFileWriterFactory(
                new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS),
                TESTING_TYPE_MANAGER,
                new NodeVersion("test_version"),
                new FileFormatDataSourceStats(),
                new OrcWriterConfig());
    }

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(((HiveColumnHandle) columnHandle).getType());
        }
        return types.build();
    }

    public static HiveRecordCursorProvider createGenericHiveRecordCursorProvider(HdfsEnvironment hdfsEnvironment)
    {
        return new GenericHiveRecordCursorProvider(hdfsEnvironment, DataSize.of(100, MEGABYTE));
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) TESTING_TYPE_MANAGER.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(keyType.getTypeSignature()),
                TypeSignatureParameter.typeParameter(valueType.getTypeSignature())));
    }

    public static ArrayType arrayType(Type elementType)
    {
        return (ArrayType) TESTING_TYPE_MANAGER.getParameterizedType(
                StandardTypes.ARRAY,
                ImmutableList.of(TypeSignatureParameter.typeParameter(elementType.getTypeSignature())));
    }

    public static RowType rowType(List<NamedTypeSignature> elementTypeSignatures)
    {
        return (RowType) TESTING_TYPE_MANAGER.getParameterizedType(
                StandardTypes.ROW,
                elementTypeSignatures.stream()
                        .map(TypeSignatureParameter::namedTypeParameter)
                        .collect(toImmutableList()));
    }

    public static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    public static Int128 longDecimal(String value)
    {
        return Decimals.valueOf(new BigDecimal(value));
    }

    public static MethodHandle distinctFromOperator(Type type)
    {
        return TESTING_TYPE_MANAGER.getTypeOperators().getDistinctFromOperator(type, simpleConvention(FAIL_ON_NULL, NULL_FLAG, NULL_FLAG));
    }

    public static boolean isDistinctFrom(MethodHandle handle, Block left, Block right)
    {
        try {
            return (boolean) handle.invokeExact(left, left == null, right, right == null);
        }
        catch (Throwable t) {
            throw new AssertionError(t);
        }
    }

    public static Object toNativeContainerValue(Type type, Object hiveValue)
    {
        if (hiveValue == null) {
            return null;
        }

        if (type instanceof ArrayType) {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Object subElement : (Iterable<?>) hiveValue) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            blockBuilder.closeEntry();
            return type.getObject(blockBuilder, 0);
        }
        if (type instanceof RowType) {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            int field = 0;
            for (Object subElement : (Iterable<?>) hiveValue) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            blockBuilder.closeEntry();
            return type.getObject(blockBuilder, 0);
        }
        if (type instanceof MapType) {
            BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
            BlockBuilder subBlockBuilder = blockBuilder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) hiveValue).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            blockBuilder.closeEntry();
            return type.getObject(blockBuilder, 0);
        }
        if (type instanceof BooleanType) {
            return hiveValue;
        }
        if (type instanceof TinyintType) {
            return (long) (byte) hiveValue;
        }
        if (type instanceof SmallintType) {
            return (long) (short) hiveValue;
        }
        if (type instanceof IntegerType) {
            return (long) (int) hiveValue;
        }
        if (type instanceof BigintType) {
            return hiveValue;
        }
        if (type instanceof RealType) {
            return (long) Float.floatToRawIntBits((float) hiveValue);
        }
        if (type instanceof DoubleType) {
            return hiveValue;
        }
        if (type instanceof VarcharType) {
            return Slices.utf8Slice(hiveValue.toString());
        }
        if (type instanceof VarbinaryType) {
            return Slices.wrappedBuffer((byte[]) hiveValue);
        }
        if (type instanceof UuidType) {
            return javaUuidToTrinoUuid(uuidFromBytes((byte[]) hiveValue));
        }
        if (type instanceof DateType) {
            return (long) ((Date) hiveValue).toEpochDay();
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private static UUID uuidFromBytes(byte[] bytes)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long msb = buffer.getLong();
        long lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }
}
