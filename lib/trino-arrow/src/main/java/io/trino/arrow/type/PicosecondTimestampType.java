package io.trino.arrow.type;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

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
        if (!(other instanceof PicosecondTimestampType that)) {
            return false;
        }
        return Objects.equals(this.timezone, that.timezone);
    }

    @Override
    public FieldVector getNewVector(String name, FieldType fieldType, BufferAllocator allocator)
    {
        StructVector vector = new StructVector(name, allocator, fieldType, null);
        vector.addOrGet("timestamp", FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, timezone)), BigIntVector.class);
        vector.addOrGet("pico_adjustment", FieldType.notNullable(new ArrowType.Int(32, true)), IntVector.class);
        return new PicosecondTimestampVector(name, allocator, vector);
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
