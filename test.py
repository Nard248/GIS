from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Database connection parameters
db_url = "postgresql://postgresqlwireless2020:software2020!!@wirelesspostgresqlflexible.postgres.database.azure.com:5432/wiroidb2"


# Create an engine
engine = create_engine(db_url)

# Create a metadata instance
metadata = MetaData()

# Variables
current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
table_name = f"temp_fips_{current_timestamp}"
provider_id = [130370]  # Replace with your provider IDs
state = ['AR']  # Replace with your state abbreviations

# Define the table
temp_fips_table = Table(
    table_name, metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('county_fips', String(255))
)

# Create the table in the database
metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Insert data into the table
insert_data_query = f"""
INSERT INTO {table_name} (county_fips)
SELECT DISTINCT fips_code
FROM us_census_block_data cb
WHERE provider_id = ANY(:provider_id) AND cb.state_abbr = ANY(:state);
"""

# Execute the insert query
session.execute(
    text(insert_data_query),
    {'provider_id': provider_id, 'state': state}
)
session.commit()

# Close the session
session.close()

print(f"Table {table_name} created and data inserted successfully.")
