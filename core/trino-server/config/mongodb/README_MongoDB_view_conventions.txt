README_MongoDB_view_conventions.txt

This document contains our conventions for creating MongoDB views for Trino.


STRUCTURE

MongoDB views for Trino should "flatten" hierarchical documents so that all fields are either scalar values or arrays of scalar values. While Trino has functions to query JSON, using the MongoDB engine for this functionality is more efficient.


IDENTIFIERS

Identifiers must be Trino SQL compatible:
- Only lower case alphanumeric, plus underscore.
- First character a-z.
- Words separated by underscores.


VIEW NAMES

View names should begin with "trino_", followed by a brief descriptive name. In general, names should be derived from their corresponding MongoDB collection names, with underscores separating words at capitalized letters in the CollectionName.

Exceptions:
- Views on "...Conf" collections should not include "_conf" in their names when the collection represents entity instances.
  - For example, a view for the "DeviceGroupConf" collection should be named "trino_device_group".
  - The removal of "_conf" should be done for the corresponding field names, too.


FIELD NAMES

Field names should generally follow the names of the MongoDB field from which they are derived, with underscores separating words at capitalized letters in the fieldName.

Exceptions:
- "_conf" is removed from fieldnames to match changes to view names, as appropriate.
- "belong" is removed from names.
  - The "belong" convention appears to indicate a foreign-key-like field that should have one row in the foreign collection. The convention is not consistently followed, so we're not using it.
- Columns with a data type of "Date" are named like "..._tstz".
  - The "Date" data type is actually a timestamp with time zone, and "tstz" abbreviates this.
- Columns in an entity collection with names like "<entity><Attribute>" are named like "<attribute>".
  - Example mapping: Patient.patientName => trino_patient.name
  - The one exception to this is the UUID column. Example mapping: patientUUID => patient_uuid
- Array columns have plural names.
- Date-time-ish fields have suffixes denoting the value type: _date, _ts (timestamp), _tstz (timestamp with time zone), _timepoint
- Columns derived from dynamic data have names derived from the metadata instanceName.
  - Exception: "DateOfBirth" => "birth_date". This follows the convention for naming date-time-ish fields.

Suffixes
- Field names should include appropriate suffixes to indicate what the field values represent. Standard suffixes we use are:
  - Identifiers
    - uuid:   UUID
    - id:     arbitrary integer identifier
    - code:   arbitrary varchar identifier
  - Display values (friendly, with human meaning)
    - abbr:   very short abbreviation
    - name:   short name, with words separated by spaces
    - desc:   longer description, with words separate by spaces
  - Date-ish values
    - date:   date
    - ts:     timestamp without time zone
    - tstz:   timestamp with time zone
    - tp:     time point (milliseconds since 1970-01-01 00:00:00.000)


CREATE VIEW FILE

- Name the create view file like "<view_name>.mql".
- File template (include a final empty line):
//  <view-description>.
//  One row per <row-definition>.
//
//  - <notes-if-needed>.

db.<view-name>.drop();

db.createView(
  "<view-name>",
  "<base-collection>",
  [<pipeline>]
);
