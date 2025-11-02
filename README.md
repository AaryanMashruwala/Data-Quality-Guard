# Data-Quality-Guard
A tiny data pipeline that:  loads CSV files into a lightweight database,  runs automatic checks (like “no negative prices”, “no missing IDs”, “orders always reference a real customer”),  fails the build and prints a report if anything looks wrong.
