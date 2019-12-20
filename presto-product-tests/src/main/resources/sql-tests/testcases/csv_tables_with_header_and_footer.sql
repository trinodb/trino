-- database: presto; tables: csv_with_header, csv_with_footer, csv_with_header_and_footer; groups: storage_formats;
--! name: Simple scan from table with Header
SELECT * FROM csv_with_header
--!
10|value2
--! name: Simple scan from table with Footer
SELECT * FROM csv_with_footer
--!
10|value1
--! name: Simple scan from table with Header and Footer
SELECT * FROM csv_with_header_and_footer
--!
10|value3
