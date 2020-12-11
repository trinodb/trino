/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.prestoconnector;

import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.LongReadFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.prestosql.spi.type.DateType.DATE;

final class PrestoColumnMappings
{
    private PrestoColumnMappings() {}

    public static ColumnMapping prestoDateColumnMapping()
    {
        return ColumnMapping.longMapping(
                DATE,
                new LongReadFunction()
                {
                    @Override
                    public boolean isNull(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        // resultSet.getObject fails for certain dates
                        // TODO simplify this read function once https://github.com/prestosql/presto/issues/6242 is fixed
                        resultSet.getString(columnIndex);
                        return resultSet.wasNull();
                    }

                    @Override
                    public long readLong(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        LocalDate localDate = LocalDate.parse(resultSet.getString(columnIndex));
                        return localDate.toEpochDay();
                    }
                },
                dateWriteFunction());
    }
}
