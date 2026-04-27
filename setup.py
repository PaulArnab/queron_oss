from setuptools import find_packages, setup


setup(
    name="queron",
    version="0.0.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "queron": [
            "graph_dist/*",
            "graph_dist/assets/*",
            "graph_dist/connector-icons/*",
        ],
    },
    entry_points={
        "console_scripts": [
            "queron=queron.cli:main",
        ],
    },
    py_modules=[
        "base",
        "db2",
        "db2_core",
        "duckdb_core",
        "duckdb_driver",
        "mssql_core",
        "mariadb_core",
        "mysql_core",
        "oracle_core",
        "postgres",
        "postgres_core",
        "type_mapper",
    ],
)
