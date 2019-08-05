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
package io.prestosql.plugin.hive.acid;

import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeName;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.orc.acid.AcidOrcPageSourceFactory;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.HiveTestUtils.createTestHdfsEnvironment;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;

public class AcidPageProcessorProvider
{
    public static final OrcReaderConfig ORC_CONFIG = new OrcReaderConfig();
    private static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment();

    private AcidPageProcessorProvider()
    {
    }

    public static ConnectorPageSource getAcidPageSource(String fileName, List<String> columnNames, List<Type> columnTypes)
    {
        return getAcidPageSource(fileName, columnNames, columnTypes, TupleDomain.all());
    }

    public static ConnectorPageSource getAcidPageSource(String fileName, List<String> columnNames, List<Type> columnTypes, TupleDomain<HiveColumnHandle> tupleDomain)
    {
        File targetFile = new File((Thread.currentThread().getContextClassLoader().getResource(fileName).getPath()));
        ImmutableList.Builder<HiveColumnHandle> builder = ImmutableList.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            Type columnType = columnTypes.get(i);
            builder.add(new HiveColumnHandle(
                    columnNames.get(i),
                    toHiveType(new HiveTypeTranslator(), columnType),
                    columnType.getTypeSignature(),
                    0,
                    REGULAR,
                    Optional.empty()));
        }
        List<HiveColumnHandle> columns = builder.build();

        HivePageSourceFactory pageSourceFactory = new AcidOrcPageSourceFactory(TYPE_MANAGER, ORC_CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats());

        Configuration config = new JobConf(new Configuration(false));
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        return pageSourceFactory.createPageSource(
                config,
                SESSION,
                new Path(targetFile.getAbsolutePath()),
                0,
                targetFile.length(),
                targetFile.length(),
                createSchema(HiveStorageFormat.ORC, columnNames, columnTypes),
                columns,
                tupleDomain,
                DateTimeZone.forID(SESSION.getTimeZoneKey().getId()),
                Optional.empty()).get();
    }

    private static Properties createSchema(HiveStorageFormat format, List<String> columnNames, List<Type> columnTypes)
    {
        Properties schema = new Properties();
        TypeTranslator typeTranslator = new HiveTypeTranslator();
        schema.setProperty(SERIALIZATION_LIB, format.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, format.getInputFormat());
        schema.setProperty(META_TABLE_COLUMNS, columnNames.stream()
                .collect(joining(",")));
        schema.setProperty(META_TABLE_COLUMN_TYPES, columnTypes.stream()
                .map(type -> toHiveType(typeTranslator, type))
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        schema.setProperty(TABLE_IS_TRANSACTIONAL, "true");
        return schema;
    }
}
