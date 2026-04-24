from pathlib import Path

import duckdb


# Create the database file inside the project's data folder.
DB_PATH = Path("data/retailstream.duckdb")


def main() -> None:
    # Ensure the parent folder exists before connecting.
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB. This creates the database file if it does not exist.
    conn = duckdb.connect(str(DB_PATH))

    try:
        # Create a simple test table for checking the connection.
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS test_connection (
                id INTEGER,
                message VARCHAR
            )
            """
        )

        # Start fresh so repeated runs always show the same test output.
        conn.execute("DELETE FROM test_connection")

        # Insert a few sample rows.
        conn.execute(
            """
            INSERT INTO test_connection (id, message)
            VALUES
                (1, 'DuckDB connection successful'),
                (2, 'RetailStream database initialized'),
                (3, 'Test table is ready')
            """
        )

        # Query the table and print the results.
        results = conn.execute(
            "SELECT id, message FROM test_connection ORDER BY id"
        ).fetchall()

        print(f"Connected to DuckDB database: {DB_PATH}")
        print("\nResults from test_connection:")
        for row in results:
            print(row)
    finally:
        # Always close the connection when finished.
        conn.close()
        print("\nDuckDB connection closed.")


if __name__ == "__main__":
    main()
