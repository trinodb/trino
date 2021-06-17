/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.dynamodb;

import com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest;
import io.trino.testing.QueryRunner;

public class TestDynamoDbTableScanRedirection
        extends AbstractTableScanRedirectionTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingDynamoDbServer server = closeAfterClass(new TestingDynamoDbServer());
        return DynamoDbQueryRunner.builder(server.getEndpointUrl(), server.getSchemaDirectory())
                .addConnectorProperties(getRedirectionProperties("dynamodb", "amazondynamodb"))
                .build();
    }
}
