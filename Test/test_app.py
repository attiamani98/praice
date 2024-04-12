import pytest
from fastapi.app import get_prices
import asyncpg
import os 
import asyncio

@pytest.fixture(scope="session")
async def db_connection():
    # Establish a connection to your database using asyncpg
    connection = await asyncpg.connect(os.environ["DATABASE_URL"])
    yield connection
    await connection.close()

@pytest.mark.asyncio
async def test_get_prices_with_data(db_connection):
    # Call the function being tested with the actual database connection
    result = await get_prices(db_connection)

    # Assert that the function returns the expected result when there is data in the database
    assert isinstance(result, dict)
    assert len(result) > 0  # Ensure that the result is not empty

