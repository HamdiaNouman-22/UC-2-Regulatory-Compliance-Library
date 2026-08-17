"""
check_db_source_systems.py
==========================
Shows all distinct source_system values in your regulations table
for CBB, so we can confirm exact names before deleting.
"""

import os
from dotenv import load_dotenv
from storage.mssql_repo import MSSQLRepository  # adjust import

load_dotenv()

repo = MSSQLRepository({
    "driver":   os.getenv("MSSQL_DRIVER"),
    "server":   os.getenv("MSSQL_SERVER"),
    "database": os.getenv("MSSQL_DATABASE"),
    "trusted_connection": "yes"
})

print("=== All source_system values for CBB ===")

query_1 = """
SELECT source_system, COUNT(*) as cnt
FROM regulations
WHERE regulator = 'Central Bank of Bahrain'
GROUP BY source_system
ORDER BY cnt DESC
"""

rows = repo.execute_query(query_1)  # or repo.execute_query()

for row in rows:
    print(f"  {row.source_system!r:40s}  {row.cnt:,} rows")


print("\n=== Sample titles per source_system ===")

query_2 = """
SELECT DISTINCT source_system, title
FROM regulations
WHERE regulator = 'Central Bank of Bahrain'
ORDER BY source_system
"""

rows = repo.execute_query(query_2)

current = None
count = 0

for row in rows:
    if row.source_system != current:
        current = row.source_system
        count = 0
        print(f"\n  [{current}]")

    if count < 3:
        print(f"    - {row.title[:80] if row.title else 'NULL'}")

    count += 1
