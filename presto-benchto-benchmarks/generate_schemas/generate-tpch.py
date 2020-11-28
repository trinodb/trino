#!/usr/bin/env python

schemas = [
    # (new_schema, source_schema)
    ('tpch_sf300_orc', 'tpch.sf300'),
    ('tpch_sf1000_orc', 'tpch.sf1000'),
    ('tpch_sf3000_orc', 'tpch.sf3000'),

    ('tpch_sf300_text', 'hive.tpch_sf300_orc'),
    ('tpch_sf1000_text', 'hive.tpch_sf1000_orc'),
    ('tpch_sf3000_text', 'hive.tpch_sf3000_orc'),
]

tables = [
    'customer',
    'lineitem',
    'nation',
    'orders',
    'part',
    'partsupp',
    'region',
    'supplier',
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
