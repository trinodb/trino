package io.trino.plugin.hudi.query.index;

import io.trino.spi.connector.ColumnHandle;

/**
 * Test implementation of ColumnHandle for unit tests
 */
public class TestingColumnHandle
        implements ColumnHandle
{
    private final String name;

    public TestingColumnHandle(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TestingColumnHandle other = (TestingColumnHandle) obj;
        return name.equals(other.name);
    }

    @Override
    public String toString()
    {
        return "Column:" + name;
    }
}
