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

import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.hive.hcatalog.data.JsonSerDe;

import java.util.List;

import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;

public enum HiveStorageFormat
{
    ORC(
            OrcSerde.class.getName(),
            OrcInputFormat.class.getName(),
            OrcOutputFormat.class.getName(),
            DataSize.of(256, Unit.MEGABYTE),
            ".orc"),
    PARQUET(
            ParquetHiveSerDe.class.getName(),
            MapredParquetInputFormat.class.getName(),
            MapredParquetOutputFormat.class.getName(),
            DataSize.of(128, Unit.MEGABYTE),
            ".parquet"),
    AVRO(
            AvroSerDe.class.getName(),
            AvroContainerInputFormat.class.getName(),
            AvroContainerOutputFormat.class.getName(),
            DataSize.of(64, Unit.MEGABYTE),
            ".avro"),
    RCBINARY(
            LazyBinaryColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName(),
            DataSize.of(8, Unit.MEGABYTE),
            ".rc"),
    RCTEXT(
            ColumnarSerDe.class.getName(),
            RCFileInputFormat.class.getName(),
            RCFileOutputFormat.class.getName(),
            DataSize.of(8, Unit.MEGABYTE),
            ".rc"),
    SEQUENCEFILE(
            LazySimpleSerDe.class.getName(),
            SequenceFileInputFormat.class.getName(),
            HiveSequenceFileOutputFormat.class.getName(),
            DataSize.of(8, Unit.MEGABYTE),
            ".seq"),
    JSON(
            JsonSerDe.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName(),
            DataSize.of(8, Unit.MEGABYTE),
            ".json"),
    TEXTFILE(
            LazySimpleSerDe.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName(),
            DataSize.of(8, Unit.MEGABYTE),
            ".txt"),
    CSV(
            OpenCSVSerde.class.getName(),
            TextInputFormat.class.getName(),
            HiveIgnoreKeyTextOutputFormat.class.getName(),
            DataSize.of(8, Unit.MEGABYTE),
            ".csv");

    private final String serde;
    private final String inputFormat;
    private final String outputFormat;
    private final DataSize estimatedWriterSystemMemoryUsage;
    private final String fileExtension;

    HiveStorageFormat(String serde, String inputFormat, String outputFormat, DataSize estimatedWriterSystemMemoryUsage, String fileExtension)
    {
        this.serde = requireNonNull(serde, "serde is null");
        this.inputFormat = requireNonNull(inputFormat, "inputFormat is null");
        this.outputFormat = requireNonNull(outputFormat, "outputFormat is null");
        this.estimatedWriterSystemMemoryUsage = requireNonNull(estimatedWriterSystemMemoryUsage, "estimatedWriterSystemMemoryUsage is null");
        this.fileExtension = requireNonNull(fileExtension, "fileExtension is null");
    }

    public String getSerDe()
    {
        return serde;
    }

    public String getInputFormat()
    {
        return inputFormat;
    }

    public String getOutputFormat()
    {
        return outputFormat;
    }

    public DataSize getEstimatedWriterSystemMemoryUsage()
    {
        return estimatedWriterSystemMemoryUsage;
    }

    public String getFileExtension()
    {
        return fileExtension;
    }

    public void validateColumns(List<HiveColumnHandle> handles)
    {
        if (this == AVRO) {
            for (HiveColumnHandle handle : handles) {
                if (!handle.isPartitionKey()) {
                    validateAvroType(handle.getHiveType().getTypeInfo(), handle.getName());
                }
            }
        }
    }

    private static void validateAvroType(TypeInfo type, String columnName)
    {
        if (type.getCategory() == Category.MAP) {
            TypeInfo keyType = mapTypeInfo(type).getMapKeyTypeInfo();
            if ((keyType.getCategory() != Category.PRIMITIVE) ||
                    (primitiveTypeInfo(keyType).getPrimitiveCategory() != PrimitiveCategory.STRING)) {
                throw new PrestoException(NOT_SUPPORTED, format("Column %s has a non-varchar map key, which is not supported by Avro", columnName));
            }
        }
        else if (type.getCategory() == Category.PRIMITIVE) {
            PrimitiveCategory primitive = primitiveTypeInfo(type).getPrimitiveCategory();
            if (primitive == PrimitiveCategory.BYTE) {
                throw new PrestoException(NOT_SUPPORTED, format("Column %s is tinyint, which is not supported by Avro. Use integer instead.", columnName));
            }
            if (primitive == PrimitiveCategory.SHORT) {
                throw new PrestoException(NOT_SUPPORTED, format("Column %s is smallint, which is not supported by Avro. Use integer instead.", columnName));
            }
        }
    }

    private static PrimitiveTypeInfo primitiveTypeInfo(TypeInfo typeInfo)
    {
        return (PrimitiveTypeInfo) typeInfo;
    }

    private static MapTypeInfo mapTypeInfo(TypeInfo typeInfo)
    {
        return (MapTypeInfo) typeInfo;
    }

    public static String getFileExtension(JobConf conf, StorageFormat storageFormat)
    {
        // text format files must have the correct extension when compressed
        if (!HiveConf.getBoolVar(conf, COMPRESSRESULT) || !HiveIgnoreKeyTextOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return storageFormat.getFileExtension();
        }

        String compressionCodecClass = conf.get("mapred.output.compression.codec");
        if (compressionCodecClass == null) {
            return new DefaultCodec().getDefaultExtension();
        }

        try {
            Class<? extends CompressionCodec> codecClass = conf.getClassByName(compressionCodecClass).asSubclass(CompressionCodec.class);
            return ReflectionUtil.newInstance(codecClass, conf).getDefaultExtension();
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Compression codec not found: " + compressionCodecClass, e);
        }
        catch (RuntimeException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Failed to load compression codec: " + compressionCodecClass, e);
        }
    }
}
