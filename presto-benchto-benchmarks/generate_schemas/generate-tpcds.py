#!/usr/bin/env python

schemas = [
    # (new_schema, source_schema)
    ('tpcds_sf10_orc', 'tpcds.sf10'),
    ('tpcds_sf30_orc', 'tpcds.sf30'),
    ('tpcds_sf100_orc', 'tpcds.sf100'),
    ('tpcds_sf300_orc', 'tpcds.sf300'),
    ('tpcds_sf1000_orc', 'tpcds.sf1000'),
    ('tpcds_sf3000_orc', 'tpcds.sf3000'),
    ('tpcds_sf10000_orc', 'tpcds.sf10000'),
]

tables = [
    'call_center',
    'catalog_page',
    'catalog_returns',
    'catalog_sales',
    'customer',
    'customer_address',
    'customer_demographics',
    'date_dim',
    'household_demographics',
    'income_band',
    'inventory',
    'item',
    'promotion',
    'reason',
    'ship_mode',
    'store',
    'store_returns',
    'store_sales',
    'time_dim',
    'warehouse',
    'web_page',
    'web_returns',
    'web_sales',
    'web_site',
]

for (new_schema, source_schema) in schemas:

    if new_schema.endswith('_orc'):
        format = 'ORC'
    elif new_schema.endswith('_text'):
        format = 'TEXTFILE'
    else:
        raise ValueError(new_schema)

    print('CREATE SCHEMA hive.{};'.format(new_schema,))
    for table in tables:
        print('CREATE TABLE "hive"."{}"."{}" WITH (format = \'{}\') AS SELECT * FROM {}."{}";'.format(
              new_schema, table, format, source_schema, table))
