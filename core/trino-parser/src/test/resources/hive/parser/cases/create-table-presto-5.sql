 CREATE TABLE if not exists tmp.test (
    id string,
    sex string
 )
 WITH (
    format = 'TEXTFILE',
    textfile_field_separator = ',',
    textfile_field_separator_escape = 'm'
 )
