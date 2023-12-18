/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.salesforce;

import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;

public interface SystemTableProvider
{
    SchemaTableName getSchemaTableName();

    SystemTable create();
}
