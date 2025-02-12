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
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.avro.AvroFileWriterFactory;
import io.trino.plugin.hive.avro.AvroPageSourceFactory;
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
import io.trino.plugin.hive.parquet.ParquetFileWriterFactory;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
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

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig)
    {
        return getHiveSessionProperties(hiveConfig, new OrcReaderConfig());
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig, OrcReaderConfig orcReaderConfig)
    {
        return new HiveSessionProperties(
                hiveConfig,
                orcReaderConfig,
                new OrcWriterConfig(),
                new ParquetReaderConfig(),
                new ParquetWriterConfig());
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig, ParquetWriterConfig parquetWriterConfig)
    {
        return new HiveSessionProperties(
                hiveConfig,
                new OrcReaderConfig(),
                new OrcWriterConfig(),
                new ParquetReaderConfig(),
                parquetWriterConfig);
    }

    public static Set<HivePageSourceFactory> getDefaultHivePageSourceFactories(HdfsEnvironment hdfsEnvironment, HiveConfig hiveConfig)
    {
        return getDefaultHivePageSourceFactories(new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS), hiveConfig);
    }

    public static Set<HivePageSourceFactory> getDefaultHivePageSourceFactories(TrinoFileSystemFactory fileSystemFactory, HiveConfig hiveConfig)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        return ImmutableSet.<HivePageSourceFactory>builder()
                .add(new CsvPageSourceFactory(fileSystemFactory, hiveConfig))
                .add(new JsonPageSourceFactory(fileSystemFactory, hiveConfig))
                .add(new OpenXJsonPageSourceFactory(fileSystemFactory, hiveConfig))
                .add(new RegexPageSourceFactory(fileSystemFactory, hiveConfig))
                .add(new SimpleTextFilePageSourceFactory(fileSystemFactory, hiveConfig))
                .add(new SimpleSequenceFilePageSourceFactory(fileSystemFactory, hiveConfig))
                .add(new AvroPageSourceFactory(fileSystemFactory))
                .add(new RcFilePageSourceFactory(fileSystemFactory, hiveConfig))
                .add(new OrcPageSourceFactory(new OrcReaderConfig(), fileSystemFactory, stats, hiveConfig))
                .add(new ParquetPageSourceFactory(fileSystemFactory, stats, new ParquetReaderConfig(), hiveConfig))
                .build();
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        return getDefaultHiveFileWriterFactories(hiveConfig, new HdfsFileSystemFactory(hdfsEnvironment, HDFS_FILE_SYSTEM_STATS));
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveConfig hiveConfig, TrinoFileSystemFactory fileSystemFactory)
    {
        NodeVersion nodeVersion = new NodeVersion("test_version");
        return ImmutableSet.<HiveFileWriterFactory>builder()
                .add(new CsvFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new JsonFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new RegexFileWriterFactory())
                .add(new OpenXJsonFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new SimpleTextFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER))
                .add(new SimpleSequenceFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER, nodeVersion))
                .add(new AvroFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER, nodeVersion))
                .add(new RcFileFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER, nodeVersion, hiveConfig))
                .add(new OrcFileWriterFactory(fileSystemFactory, TESTING_TYPE_MANAGER, nodeVersion, new FileFormatDataSourceStats(), new OrcWriterConfig()))
                .add(new ParquetFileWriterFactory(fileSystemFactory, nodeVersion, TESTING_TYPE_MANAGER, hiveConfig, new FileFormatDataSourceStats()))
                .build();
    }

    public static List<Type> getTypes(List<? extends ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnHandle columnHandle : columnHandles) {
            types.add(((HiveColumnHandle) columnHandle).getType());
        }
        return types.build();
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

    public static Object toNativeContainerValue(Type type, Object hiveValue)
    {
        if (hiveValue == null) {
            return null;
        }

        if (type instanceof ArrayType arrayType) {
            Collection<?> hiveArray = (Collection<?>) hiveValue;
            return buildArrayValue(arrayType, hiveArray.size(), valueBuilder -> {
                for (Object subElement : hiveArray) {
                    appendToBlockBuilder(type.getTypeParameters().get(0), subElement, valueBuilder);
                }
            });
        }
        if (type instanceof RowType rowType) {
            return buildRowValue(rowType, fields -> {
                int fieldIndex = 0;
                for (Object subElement : (Iterable<?>) hiveValue) {
                    appendToBlockBuilder(type.getTypeParameters().get(fieldIndex), subElement, fields.get(fieldIndex));
                    fieldIndex++;
                }
            });
        }
        if (type instanceof MapType mapType) {
            Map<?, ?> hiveMap = (Map<?, ?>) hiveValue;
            return buildMapValue(
                    mapType,
                    hiveMap.size(),
                    (keyBuilder, valueBuilder) -> {
                        hiveMap.forEach((key, value) -> {
                            appendToBlockBuilder(mapType.getKeyType(), key, keyBuilder);
                            appendToBlockBuilder(mapType.getValueType(), value, valueBuilder);
                        });
                    });
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
