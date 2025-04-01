CREATE TABLE correlation_matrix (
    correlation_matrix_id TEXT NOT NULL,
    var1_nm TEXT NOT NULL,
    var2_nm TEXT NOT NULL,
    correlation_value_no REAL NOT NULL
);

-- To populate the table, use the following SQL statement after loading the CSV data:
-- Assuming you are using a database like SQLite or PostgreSQL, you can use a tool or script to import the CSV data into this table.
-- Example for SQLite:
-- .mode csv
-- .import /workspaces/SolvMate/files/correlation_matrix.csv correlation_matrix