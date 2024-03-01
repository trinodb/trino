package io.starburst.schema.discovery.models;

public record ArbitraryPath(String path)
        implements TablePath
{
    public static final ArbitraryPath ARBITRARY_PATH_EMPTY = new ArbitraryPath("");

    @Override
    public boolean isEmpty()
    {
        return ARBITRARY_PATH_EMPTY.equals(this);
    }

    @Override
    public String toString()
    {
        return this.path;
    }
}
