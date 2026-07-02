-- Keyed lookup in the materialized view (dataflow). The expected file only
-- contains the data row: the header (column names) of a view is an
-- implementation detail, so the workflow compares `tail -n +2` of the output.
SELECT * FROM chat_counts WHERE patient_id = 1;
