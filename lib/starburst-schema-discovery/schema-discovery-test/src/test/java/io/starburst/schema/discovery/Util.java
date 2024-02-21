/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.starburst.schema.discovery.formats.csv.CsvSchemaDiscovery;
import io.starburst.schema.discovery.formats.json.JsonSchemaDiscovery;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.orc.OrcSchemaDiscovery;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.ParquetSchemaDiscovery;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryInput;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.io.DiscoveryTrinoInput;
import io.starburst.schema.discovery.models.TableFormat;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.HdfsOrcDataSource;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.plugin.hive.type.TypeInfo;

import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.trino.plugin.hive.type.TypeInfoFactory.getStructTypeInfo;

public class Util
{
    public static final ParquetDataSourceFactory parquetDataSourceFactory = (inputFile) -> new TrinoParquetDataSource(inputFile, new ParquetReaderOptions(), new FileFormatDataSourceStats());
    public static final OrcDataSourceFactory orcDataSourceFactory = (id, size, options, inputFile) -> new HdfsOrcDataSource(id, size, options, inputFile, new FileFormatDataSourceStats());
    public static final ParquetSchemaDiscovery parquetSchemaDiscovery = new ParquetSchemaDiscovery(parquetDataSourceFactory);
    public static final OrcSchemaDiscovery orcSchemaDiscovery = new OrcSchemaDiscovery(orcDataSourceFactory);

    public static final Map<TableFormat, SchemaDiscovery> schemaDiscoveryInstances = ImmutableMap.of(
            TableFormat.CSV, CsvSchemaDiscovery.INSTANCE,
            TableFormat.JSON, JsonSchemaDiscovery.INSTANCE,
            TableFormat.ORC, orcSchemaDiscovery,
            TableFormat.PARQUET, parquetSchemaDiscovery);

    public static DiscoveryInput testFile(String name)
    {
        try {
            return testFile(fileSystem(), name);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Location testFilePath(String name)
    {
        try {
            URL resource = Resources.getResource(name);
            return Location.of(resource.toString().replace("file:", "local://"));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static DiscoveryInput testFile(DiscoveryTrinoFileSystem fileSystem, String name)
    {
        try {
            return new DiscoveryTrinoInput(fileSystem, testFilePath(name));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static DiscoveryTrinoFileSystem fileSystem()
    {
        return new DiscoveryTrinoFileSystem(new SortedTrinoFileSystem(new LocalFileSystem(Paths.get("/"))));
    }

    public static TypeInfo struct(String name1, TypeInfo type1, String name2, TypeInfo type2, String name3, TypeInfo type3)
    {
        return getStructTypeInfo(Arrays.asList(name1, name2, name3), Arrays.asList(type1, type2, type3));
    }

    public static TypeInfo struct(String name1, TypeInfo type1, String name2, TypeInfo type2)
    {
        return getStructTypeInfo(Arrays.asList(name1, name2), Arrays.asList(type1, type2));
    }

    public static TypeInfo struct(String name, TypeInfo type)
    {
        return getStructTypeInfo(ImmutableList.of(name), ImmutableList.of(type));
    }

    public static TypeInfo arrayType(TypeInfo type)
    {
        return HiveTypes.arrayType(type);
    }

    public static TypeInfo mapType(TypeInfo key, TypeInfo value)
    {
        return HiveTypes.mapType(key, value);
    }

    public static Column column(String name, TypeInfo type)
    {
        return new Column(toLowerCase(name), new HiveType(type));
    }

    public static List<Column> withoutSamples(List<Column> columns)
    {
        return columns.stream().map(Column::withoutSampleValue).collect(toImmutableList());
    }

    public static Column toColumn(String name, TypeInfo typeInfo)
    {
        return HiveTypes.toColumn(name, typeInfo).orElseThrow();
    }

    private Util()
    {
    }
}
