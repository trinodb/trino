package io.trino.plugin.teradata.util;

/**
 * Defines constants used throughout the Teradata connector.
 */
public interface TeradataConstants
{
    /**
     * The maximum length allowed for Teradata object names (e.g., databases, tables, columns).
     */
    int TERADATA_OBJECT_NAME_LIMIT = 128;
}
