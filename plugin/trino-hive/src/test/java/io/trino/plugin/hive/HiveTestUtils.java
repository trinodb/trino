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
import io.airlift.units.DataSize;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.azure.HiveAzureConfig;
import io.trino.plugin.hive.azure.TrinoAzureConfigurationInitializer;
import io.trino.plugin.hive.gcs.GoogleGcsConfigurationInitializer;
import io.trino.plugin.hive.gcs.HiveGcsConfig;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.orc.OrcPageSourceFactory;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.trino.plugin.hive.rubix.RubixEnabledConfig;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.s3select.S3SelectRecordCursorProvider;
import io.trino.plugin.hive.s3select.TrinoS3ClientFactory;
import io.trino.spi.PageSorter;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.MapType;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.hive.common.type.Timestamp;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public final class HiveTestUtils
{
    private HiveTestUtils() {}

    public static final ConnectorSession SESSION = getHiveSession(new HiveConfig());

    public static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment();

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
        return getHiveSessionProperties(hiveConfig, new RubixEnabledConfig(), orcReaderConfig);
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig, RubixEnabledConfig rubixEnabledConfig, OrcReaderConfig orcReaderConfig)
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
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        return ImmutableSet.<HivePageSourceFactory>builder()
                .add(new RcFilePageSourceFactory(TESTING_TYPE_MANAGER, hdfsEnvironment, stats, hiveConfig))
                .add(new OrcPageSourceFactory(new OrcReaderConfig(), hdfsEnvironment, stats, hiveConfig))
                .add(new ParquetPageSourceFactory(hdfsEnvironment, stats, new ParquetReaderConfig(), hiveConfig))
                .build();
    }

    public static Set<HiveRecordCursorProvider> getDefaultHiveRecordCursorProviders(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new S3SelectRecordCursorProvider(hdfsEnvironment, new TrinoS3ClientFactory(hiveConfig)))
                .build();
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        return ImmutableSet.<HiveFileWriterFactory>builder()
                .add(new RcFileFileWriterFactory(hdfsEnvironment, TESTING_TYPE_MANAGER, new NodeVersion("test_version"), hiveConfig, new FileFormatDataSourceStats()))
                .add(getDefaultOrcFileWriterFactory(hdfsEnvironment))
                .build();
    }

    private static OrcFileWriterFactory getDefaultOrcFileWriterFactory(HdfsEnvironment hdfsEnvironment)
    {
        return new OrcFileWriterFactory(
                hdfsEnvironment,
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

    private static HdfsEnvironment createTestHdfsEnvironment()
    {
        HdfsConfiguration hdfsConfig = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig(),
                        ImmutableSet.of(
                                new TrinoS3ConfigurationInitializer(new HiveS3Config()),
                                new GoogleGcsConfigurationInitializer(new HiveGcsConfig()),
                                new TrinoAzureConfigurationInitializer(new HiveAzureConfig()))),
                ImmutableSet.of());
        return new HdfsEnvironment(hdfsConfig, new HdfsConfig(), new NoHdfsAuthentication());
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
                ImmutableList.copyOf(elementTypeSignatures.stream()
                        .map(TypeSignatureParameter::namedTypeParameter)
                        .collect(toImmutableList())));
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

    public static Timestamp hiveTimestamp(LocalDateTime local)
    {
        return Timestamp.ofEpochSecond(local.toEpochSecond(ZoneOffset.UTC), local.getNano());
    }
}
