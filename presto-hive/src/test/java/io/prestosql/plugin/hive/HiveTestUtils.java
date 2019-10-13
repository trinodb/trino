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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.prestosql.PagesIndexPageSorter;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.operator.PagesIndex;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.gcs.GoogleGcsConfigurationInitializer;
import io.prestosql.plugin.hive.gcs.HiveGcsConfig;
import io.prestosql.plugin.hive.orc.OrcFileWriterFactory;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.plugin.hive.parquet.ParquetPageSourceFactory;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.plugin.hive.parquet.ParquetWriterConfig;
import io.prestosql.plugin.hive.rcfile.RcFilePageSourceFactory;
import io.prestosql.plugin.hive.s3.HiveS3Config;
import io.prestosql.plugin.hive.s3.PrestoS3ConfigurationInitializer;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.testing.TestingConnectorSession;
import io.prestosql.type.InternalTypeManager;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static java.util.stream.Collectors.toList;

public final class HiveTestUtils
{
    private HiveTestUtils() {}

    public static final ConnectorSession SESSION = getHiveSession(new HiveConfig());

    private static final Metadata METADATA = createTestMetadataManager();
    public static final TypeManager TYPE_MANAGER = new InternalTypeManager(METADATA);

    public static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment();

    public static final PageSorter PAGE_SORTER = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));

    public static ConnectorSession getHiveSession(HiveConfig hiveConfig)
    {
        return new TestingConnectorSession(getHiveSessionProperties(hiveConfig).getSessionProperties());
    }

    public static HiveSessionProperties getHiveSessionProperties(HiveConfig hiveConfig)
    {
        return new HiveSessionProperties(
                hiveConfig,
                new OrcReaderConfig(),
                new OrcWriterConfig(),
                new ParquetReaderConfig(),
                new ParquetWriterConfig());
    }

    public static Set<HivePageSourceFactory> getDefaultHivePageSourceFactories(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        return ImmutableSet.<HivePageSourceFactory>builder()
                .add(new RcFilePageSourceFactory(TYPE_MANAGER, hdfsEnvironment, stats))
                .add(new OrcPageSourceFactory(new OrcReaderConfig(), hdfsEnvironment, stats))
                .add(new ParquetPageSourceFactory(TYPE_MANAGER, hdfsEnvironment, stats))
                .build();
    }

    public static Set<HiveRecordCursorProvider> getDefaultHiveRecordCursorProvider(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        return ImmutableSet.<HiveRecordCursorProvider>builder()
                .add(new GenericHiveRecordCursorProvider(hdfsEnvironment, hiveConfig))
                .build();
    }

    public static Set<HiveFileWriterFactory> getDefaultHiveFileWriterFactories(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        return ImmutableSet.<HiveFileWriterFactory>builder()
                .add(new RcFileFileWriterFactory(hdfsEnvironment, TYPE_MANAGER, new NodeVersion("test_version"), hiveConfig, new FileFormatDataSourceStats()))
                .add(getDefaultOrcFileWriterFactory(hiveConfig, hdfsEnvironment))
                .build();
    }

    public static OrcFileWriterFactory getDefaultOrcFileWriterFactory(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        return new OrcFileWriterFactory(
                hdfsEnvironment,
                TYPE_MANAGER,
                new NodeVersion("test_version"),
                hiveConfig,
                new OrcWriterConfig(),
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
        return new GenericHiveRecordCursorProvider(hdfsEnvironment, new DataSize(100, MEGABYTE));
    }

    public static HdfsEnvironment createTestHdfsEnvironment()
    {
        HdfsConfiguration hdfsConfig = new HiveHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig(),
                        ImmutableSet.of(
                                new PrestoS3ConfigurationInitializer(new HiveS3Config()),
                                new GoogleGcsConfigurationInitializer(new HiveGcsConfig()))),
                ImmutableSet.of());
        return new HdfsEnvironment(hdfsConfig, new HdfsConfig(), new NoHdfsAuthentication());
    }

    public static MapType mapType(Type keyType, Type valueType)
    {
        return (MapType) METADATA.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.typeParameter(keyType.getTypeSignature()),
                TypeSignatureParameter.typeParameter(valueType.getTypeSignature())));
    }

    public static ArrayType arrayType(Type elementType)
    {
        return (ArrayType) METADATA.getParameterizedType(
                StandardTypes.ARRAY,
                ImmutableList.of(TypeSignatureParameter.typeParameter(elementType.getTypeSignature())));
    }

    public static RowType rowType(List<NamedTypeSignature> elementTypeSignatures)
    {
        return (RowType) METADATA.getParameterizedType(
                StandardTypes.ROW,
                ImmutableList.copyOf(elementTypeSignatures.stream()
                        .map(TypeSignatureParameter::namedTypeParameter)
                        .collect(toList())));
    }

    public static Long shortDecimal(String value)
    {
        return new BigDecimal(value).unscaledValue().longValueExact();
    }

    public static Slice longDecimal(String value)
    {
        return encodeScaledValue(new BigDecimal(value));
    }

    public static MethodHandle distinctFromOperator(Type type)
    {
        ResolvedFunction function = METADATA.resolveOperator(IS_DISTINCT_FROM, ImmutableList.of(type, type));
        return METADATA.getScalarFunctionImplementation(function).getMethodHandle();
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
}
