package io.trino.arrow.type;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.Arrays;
import java.util.Objects;

public class PicosecondTimestampType extends ArrowType.ExtensionType
{
    public static final String EXTENSION_NAME = "trino.timestamp.pico";
    private final String timezone;

    public PicosecondTimestampType(String timezone)
    {
        super();
        this.timezone = timezone;
    }

    private static ArrowType createStorageType()
    {
        return new ArrowType.Struct();
    }

    public static java.util.List<Field> getStorageFields()
    {
        return Arrays.asList(
            new Field("micros", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("picoAdjustment", FieldType.notNullable(new ArrowType.Int(32, true)), null)
        );
    }

    @Override
    public String extensionName()
    {
        return EXTENSION_NAME;
    }

    @Override
    public ArrowType storageType()
    {
        return new ArrowType.Struct();
    }

    @Override
    public String serialize()
    {
        return timezone == null ? "" : timezone;
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData)
    {
        return new PicosecondTimestampType(serializedData.isEmpty() ? null : serializedData);
    }

    @Override
    public boolean extensionEquals(ExtensionType other)
    {
        if (!(other instanceof PicosecondTimestampType)) {
            return false;
        }
        PicosecondTimestampType that = (PicosecondTimestampType) other;
        return Objects.equals(this.timezone, that.timezone);
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator)
    {
        StructVector vector = new StructVector(name, allocator, fieldType, null);
        vector.addOrGet("micros", FieldType.notNullable(new ArrowType.Int(64, true)), BigIntVector.class);
        vector.addOrGet("picoAdjustment", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        return vector;
    }

    public String getTimezone()
    {
        return timezone;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PicosecondTimestampType that = (PicosecondTimestampType) o;
        return Objects.equals(timezone, that.timezone);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timezone);
    }
}
