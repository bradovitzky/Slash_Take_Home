import duckdb

# Database Test Functions
def create_test_table():
    """Creates a test table"""

    conn = duckdb.connect('dev.duckdb')
    # create test table with dummy columns for duckdb testing
    conn.execute("""
        CREATE TABLE test1(
            id VARCHAR(50) UNIQUE NOT NULL,
            value1 VARCHAR(50) NOT NULL
                          )
    """)
    conn.close()
def add_rows_to_test_table():
    """Inserts sample data into the test table"""
    conn = duckdb.connect('dev.duckdb')
    conn.execute("""
        INSERT INTO test1 (id, value1)
        VALUES
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3000)
    """)
    conn.close()
def check_test_table_contents():
    """Prints the contents of the test table"""
    conn = duckdb.connect('dev.duckdb')
    result = conn.execute("""SELECT * FROM test1""")
    print(result)
    conn.close()
def drop_test_table():
    """Drops the test table from the database"""
    conn = duckdb.connect('dev.duckdb')
    conn.execute("""DROP TABLE test1""")
    print("test1 table dropped successfully")
    conn.close()

def main():    
    conn = duckdb.connect('dev.duckdb')
    
    result = conn.execute("SHOW TABLES").fetchall()
    
    print("- Tables in dev.duckdb:", result)
    for table in result:
        # Select the head of each table
        head_result = conn.execute(f"SELECT * FROM {table[0]} LIMIT 5").fetchall()
        print(f"- Head of {table[0]}:")
        for row in head_result:
            print(f"    - {row}")
        print("\n----\n")
    conn.close()

    print(show_enrollment_costs())
    
    # events = get_events()
    # transactions = get_transactions()
    # entities = get_entities()
    # print("Events example:\n", events[0])
    # print("Transactions example:\n", transactions[0])
    # print("Entities example:\n",entities[0])

    conn = duckdb.connect('dev.duckdb')
    print(conn.execute("SELECT DISTINCT card_status FROM card_events").fetchall())
    conn.close()
    """
    Events example:
    {'SLASH_ACCOUNT_ID': 'sa_7r1z5obxg0v8', 'CARD_EVENT_ID': 'ce_1l5a1kttd9cqi', 'CARD_ID': 'c_p3cfa3lo4hvz', 'EVENT_TYPE': 'update', 'CARD_STATUS': 'active', 'TIMESTAMP': datetime.datetime(2024, 7, 21, 21, 50, 14, 817000)}
    Transactions example:
    {'TRANSACTION_ID': 'te_card_10003l0ftibyu', 'SUBACCOUNT_ID': 'sa_vtzf9ugf6bj7', 'CARD_ID': 'c_2a02q7rl1sv75', 'ACCOUNT_TYPE': 'commercial', 'TRANSACTION_AMOUNT': -5.0, 'ORIGINAL_CURRENCY_CODE': 'USD', 'MERCHANT_DESCRIPTION': 'VISIBLE', 'MERCHANT_CATEGORY_CODE': None, 'MERCHANT_COUNTRY': 'US', 'TIMESTAMP': datetime.datetime(2024, 4, 29, 1, 58, 23, 164000)}
    Entities example:
    {'ENTITY_ID': 'le_38vbqyta9f7dq', 'SUBACCOUNT_ID': 'sa_3ljglw4fm68ux', 'ACCOUNT_CREATION_DATE': datetime.datetime(2024, 5, 2, 17, 55, 32, 608000)}
    """

def get_entities() -> list:
    """Fetches all records from the 'entity' table in the DuckDB database and returns them as a list of dictionaries."""
    conn = duckdb.connect('dev.duckdb')
    result = conn.execute(f"SELECT * FROM entity")
    
    # Get column names from the query
    columns = [description[0] for description in result.description]
    
    # Fetch all rows from the result
    rows = result.fetchall()

    # Convert rows into a list of dictionaries
    entities = [dict(zip(columns, row)) for row in rows]
    
    return entities

def get_events() -> list:
    """Fetches all records from the 'card_events' table in the DuckDB database and returns them as a list of dictionaries."""
    conn = duckdb.connect('dev.duckdb')
    result = conn.execute(f"SELECT * FROM card_events")
    
    # Get column names from the query
    columns = [description[0] for description in result.description]
    
    # Fetch all rows from the result
    rows = result.fetchall()

    # Convert rows into a list of dictionaries
    events = [dict(zip(columns, row)) for row in rows]
    
    return events

def get_transactions() -> list:
    """Fetches all records from the 'card_transactions' table in the DuckDB database and returns them as a list of dictionaries."""
    conn = duckdb.connect('dev.duckdb')
    result = conn.execute(f"SELECT * FROM card_transactions")
    
    # Get column names from the query
    columns = [description[0] for description in result.description]
    
    # Fetch all rows from the result
    rows = result.fetchall()

    # Convert rows into a list of dictionaries
    transactions = [dict(zip(columns, row)) for row in rows]
    
    return transactions

def show_enrollment_costs():
    conn = duckdb.connect('dev.duckdb')
    result = conn.execute("SELECT * FROM enrollment_costs").fetchall()
    conn.close()
    return result

def show_revenue_benefits():
    conn = duckdb.connect('dev.duckdb')
    result = conn.execute("SELECT * FROM revenu").fetchall()
    conn.close()
    return result


def export_tables_to_csvs():
    conn = duckdb.connect('dev.duckdb')

    gmv_df = conn.execute("SELECT * FROM monthly_processing_volume").fetchdf()
    # segmentation_df = conn.execute("SELECT * FROM customer_segmentation").fetchdf()
    concentration_df = conn.execute("SELECT * FROM customer_concentration").fetchdf()
    utilization_df = conn.execute("SELECT * FROM card_utilization").fetchdf()
    top_merchants_df = conn.execute("SELECT * FROM top_merchants_by_volume").fetchdf()
    SCP_analysis_df = conn.execute("SELECT * FROM SCP_analysis").fetchdf()
    GMV_cohort_analysis_df = conn.execute("SELECT * FROM GMV_cohort_analysis").fetchdf()
    average_card_spend_df = conn.execute("SELECT * FROM average_card_spend").fetchdf()
    entity_level_card_metrics_df = conn.execute("SELECT * FROM entity_level_card_metrics").fetchdf()

    #export dfs to csvs for tableau ingestion
    gmv_df.to_csv('output_csvs/monthly_processing_volume.csv', index=False)
    # segmentation_df.to_csv('output_csvs/customer_segmentation.csv', index=False)
    concentration_df.to_csv('output_csvs/customer_concentration.csv', index=False)
    utilization_df.to_csv('output_csvs/card_utilization.csv', index=False)
    top_merchants_df.to_csv('output_csvs/top_merchants_by_volume.csv', index=False)
    SCP_analysis_df.to_csv('output_csvs/SCP_analysis.csv', index=False)
    GMV_cohort_analysis_df.to_csv('output_csvs/GMV_cohort_analysis.csv', index=False)
    average_card_spend_df.to_csv('output_csvs/average_card_spend.csv', index=False)
    entity_level_card_metrics_df.to_csv('output_csvs/entity_level_card_metrics.csv', index=False)
    conn.close()

if __name__ == "__main__":
    # create_test_table()
    # add_rows_to_test_table()
    # check_test_table_contents()
    # drop_test_table()

    # main()
    export_tables_to_csvs()

