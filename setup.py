from setuptools import find_packages, setup


setup(
    name="queron",
    version="0.0.0",
    packages=find_packages(),
    include_package_data=True,
    py_modules=[
        "base",
        "db2",
        "db2_core",
        "duckdb_core",
        "duckdb_driver",
        "mssql_core",
        "postgres",
        "postgres_core",
        "type_mapper",
    ],
)
