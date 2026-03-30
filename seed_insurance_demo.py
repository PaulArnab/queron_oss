"""
Reset and seed the local Postgres and Db2 demo databases with a shared
insurance-company dataset for notebook pipeline examples.

This script intentionally clears user tables in:
- PostgreSQL public schema
- Db2 DB2INST1 schema

The seeded model is designed for cross-system joins:
- PostgreSQL: policy, coverage, claim
- Db2: policy, person, address, phone, email

Join key:
- policy.policy_number
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import sys

import psycopg


BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import db2  # noqa: E402


POSTGRES_DSN = "postgresql://admin:password123@127.0.0.1:5432/retail_db?sslmode=disable"
DB2_CONN_STR = "DATABASE=LOOMDB;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=LoomDb2Pass123!;"
DB2_SCHEMA = "DB2INST1"


@dataclass(frozen=True)
class PersonSeed:
    person_id: int
    customer_number: str
    first_name: str
    last_name: str
    birth_date: date
    occupation: str
    annual_income: Decimal
    city: str
    state_code: str
    postal_code: str
    phone_number: str
    email_address: str


def _person_seeds() -> list[PersonSeed]:
    rows = [
        ("CUST-1001", "Amelia", "Reed", date(1986, 4, 18), "Actuary", Decimal("125000.00"), "Chicago", "IL", "60611"),
        ("CUST-1002", "Marcus", "Cole", date(1979, 7, 9), "Contractor", Decimal("148000.00"), "Dallas", "TX", "75201"),
        ("CUST-1003", "Nina", "Patel", date(1991, 2, 4), "Physician", Decimal("212000.00"), "Seattle", "WA", "98101"),
        ("CUST-1004", "Devon", "Brooks", date(1984, 9, 14), "Teacher", Decimal("83000.00"), "Columbus", "OH", "43215"),
        ("CUST-1005", "Hannah", "Kim", date(1988, 11, 28), "Attorney", Decimal("176000.00"), "Atlanta", "GA", "30303"),
        ("CUST-1006", "Luis", "Ortega", date(1976, 6, 3), "Restaurant Owner", Decimal("196000.00"), "Phoenix", "AZ", "85004"),
        ("CUST-1007", "Iris", "Nguyen", date(1993, 1, 20), "Data Analyst", Decimal("98000.00"), "Denver", "CO", "80202"),
        ("CUST-1008", "Owen", "Sullivan", date(1981, 8, 30), "Architect", Decimal("154000.00"), "Boston", "MA", "02110"),
        ("CUST-1009", "Priya", "Shah", date(1989, 3, 12), "Pharmacist", Decimal("141000.00"), "Jersey City", "NJ", "07302"),
        ("CUST-1010", "Caleb", "Foster", date(1978, 10, 7), "Logistics Manager", Decimal("117000.00"), "Nashville", "TN", "37219"),
        ("CUST-1011", "Elena", "Morales", date(1985, 5, 23), "UX Designer", Decimal("132000.00"), "Miami", "FL", "33131"),
        ("CUST-1012", "Jordan", "Bennett", date(1990, 12, 16), "Small Business Owner", Decimal("165000.00"), "Minneapolis", "MN", "55402"),
    ]
    out: list[PersonSeed] = []
    for index, row in enumerate(rows, start=1):
        customer_number, first_name, last_name, birth_date, occupation, annual_income, city, state_code, postal_code = row
        phone_number = f"555-01{index:02d}"
        email_address = f"{first_name.lower()}.{last_name.lower()}@loominsurance-demo.com"
        out.append(
            PersonSeed(
                person_id=index,
                customer_number=customer_number,
                first_name=first_name,
                last_name=last_name,
                birth_date=birth_date,
                occupation=occupation,
                annual_income=annual_income,
                city=city,
                state_code=state_code,
                postal_code=postal_code,
                phone_number=phone_number,
                email_address=email_address,
            )
        )
    return out


def _policy_rows(persons: list[PersonSeed]) -> tuple[list[tuple], list[tuple], list[tuple], list[tuple], list[tuple], list[tuple], list[tuple], list[tuple]]:
    line_cycle = [
        "AUTO",
        "HOME",
        "AUTO",
        "HOME",
        "UMBRELLA",
        "AUTO",
        "COMMERCIAL_AUTO",
        "HOME",
        "AUTO",
        "HOME",
        "UMBRELLA",
        "COMMERCIAL_PROPERTY",
        "AUTO",
        "HOME",
        "AUTO",
        "COMMERCIAL_AUTO",
        "HOME",
        "AUTO",
    ]
    status_cycle = [
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "PENDING_RENEWAL",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "CANCELLED",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
        "LAPSED",
        "ACTIVE",
        "ACTIVE",
        "ACTIVE",
    ]
    billing_plans = ["MONTHLY", "MONTHLY", "SEMI_ANNUAL", "ANNUAL"]
    contact_channels = ["EMAIL", "PHONE", "MAIL", "SMS"]
    coverage_templates = {
        "AUTO": [
            ("BI", "Bodily Injury Liability", Decimal("500000.00"), Decimal("500.00"), Decimal("680.00"), False),
            ("COMP", "Comprehensive", Decimal("75000.00"), Decimal("500.00"), Decimal("240.00"), False),
            ("COLL", "Collision", Decimal("75000.00"), Decimal("1000.00"), Decimal("315.00"), True),
        ],
        "HOME": [
            ("DWELLING", "Dwelling", Decimal("650000.00"), Decimal("2500.00"), Decimal("980.00"), False),
            ("PERSONAL_PROP", "Personal Property", Decimal("250000.00"), Decimal("1000.00"), Decimal("285.00"), False),
            ("PERSONAL_LIAB", "Personal Liability", Decimal("300000.00"), Decimal("0.00"), Decimal("145.00"), True),
        ],
        "UMBRELLA": [
            ("UMB", "Umbrella Liability", Decimal("1000000.00"), Decimal("0.00"), Decimal("420.00"), False),
            ("DEFENSE", "Defense Cost", Decimal("250000.00"), Decimal("0.00"), Decimal("90.00"), True),
        ],
        "COMMERCIAL_AUTO": [
            ("AUTO_LIAB", "Commercial Auto Liability", Decimal("1000000.00"), Decimal("1000.00"), Decimal("1260.00"), False),
            ("PHYS_DAMAGE", "Physical Damage", Decimal("180000.00"), Decimal("2500.00"), Decimal("440.00"), True),
        ],
        "COMMERCIAL_PROPERTY": [
            ("BUILDING", "Building", Decimal("1250000.00"), Decimal("5000.00"), Decimal("1880.00"), False),
            ("BPP", "Business Personal Property", Decimal("450000.00"), Decimal("2500.00"), Decimal("720.00"), False),
            ("BI", "Business Interruption", Decimal("250000.00"), Decimal("0.00"), Decimal("330.00"), True),
        ],
    }
    claim_causes = {
        "AUTO": "Collision",
        "HOME": "Water Damage",
        "UMBRELLA": "Liability",
        "COMMERCIAL_AUTO": "Fleet Accident",
        "COMMERCIAL_PROPERTY": "Wind",
    }

    policies: list[tuple] = []
    db2_policy: list[tuple] = []
    coverages: list[tuple] = []
    claims: list[tuple] = []
    addresses: list[tuple] = []
    phones: list[tuple] = []
    emails: list[tuple] = []
    persons_rows: list[tuple] = []

    base_effective = date(2024, 1, 1)
    for person in persons:
        persons_rows.append(
            (
                person.person_id,
                person.customer_number,
                person.first_name,
                person.last_name,
                person.birth_date,
                person.occupation,
                person.annual_income,
                "EN",
            )
        )
        addresses.append(
            (
                person.person_id,
                person.person_id,
                "MAILING",
                f"{100 + person.person_id} Market Street",
                None,
                person.city,
                person.state_code,
                person.postal_code,
                "US",
                1,
            )
        )
        phones.append(
            (
                person.person_id,
                person.person_id,
                "MOBILE",
                person.phone_number,
                1,
            )
        )
        emails.append(
            (
                person.person_id,
                person.person_id,
                "PRIMARY",
                person.email_address,
                1,
            )
        )

    coverage_id = 1
    claim_id = 1
    for index, line in enumerate(line_cycle, start=1):
        person = persons[(index - 1) % len(persons)]
        policy_id = index
        policy_number = f"POL-2025-{1000 + index}"
        effective_date = base_effective + timedelta(days=21 * (index - 1))
        expiration_date = effective_date + timedelta(days=364)
        premium_base = {
            "AUTO": Decimal("1235.00"),
            "HOME": Decimal("1650.00"),
            "UMBRELLA": Decimal("575.00"),
            "COMMERCIAL_AUTO": Decimal("2480.00"),
            "COMMERCIAL_PROPERTY": Decimal("3340.00"),
        }[line] + Decimal(index * 17)
        deductible = {
            "AUTO": Decimal("500.00"),
            "HOME": Decimal("2500.00"),
            "UMBRELLA": Decimal("0.00"),
            "COMMERCIAL_AUTO": Decimal("1000.00"),
            "COMMERCIAL_PROPERTY": Decimal("5000.00"),
        }[line]
        policies.append(
            (
                policy_id,
                policy_number,
                line,
                "Loom Mutual Insurance",
                status_cycle[index - 1],
                effective_date,
                expiration_date,
                premium_base,
                deductible,
                billing_plans[(index - 1) % len(billing_plans)],
                f"AGT-{person.state_code}-{(index % 4) + 1:02d}",
                person.state_code,
            )
        )
        db2_policy.append(
            (
                policy_id,
                policy_number,
                person.person_id,
                "PRIMARY_INSURED",
                effective_date,
                expiration_date,
                contact_channels[(index - 1) % len(contact_channels)],
            )
        )

        templates = coverage_templates[line]
        for offset, (coverage_code, coverage_name, coverage_limit, coverage_deductible, coverage_premium, is_optional) in enumerate(templates, start=1):
            coverages.append(
                (
                    coverage_id,
                    policy_id,
                    coverage_code,
                    coverage_name,
                    coverage_limit + Decimal(index * 1000),
                    coverage_deductible,
                    coverage_premium + Decimal(offset * 11),
                    is_optional,
                )
            )
            coverage_id += 1

        if index % 2 == 0 or line.startswith("COMMERCIAL"):
            reserve_amount = Decimal("4500.00") + Decimal(index * 850)
            paid_amount = reserve_amount * Decimal("0.45") if index % 4 != 0 else reserve_amount * Decimal("0.92")
            claim_status = "CLOSED" if index % 4 == 0 else "OPEN"
            claims.append(
                (
                    claim_id,
                    f"CLM-2025-{7000 + claim_id}",
                    policy_id,
                    effective_date + timedelta(days=45 + index),
                    effective_date + timedelta(days=48 + index),
                    claim_status,
                    claim_causes[line],
                    reserve_amount.quantize(Decimal("0.01")),
                    paid_amount.quantize(Decimal("0.01")),
                    person.state_code,
                    f"{['Maya','Ethan','Riley','Sophia'][claim_id % 4]} Adjuster",
                    (effective_date + timedelta(days=120 + index)) if claim_status == "CLOSED" else None,
                )
            )
            claim_id += 1

    return policies, coverages, claims, persons_rows, addresses, phones, emails, db2_policy


def _reset_postgres(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS public CASCADE")
        cur.execute("CREATE SCHEMA public")
        cur.execute("GRANT ALL ON SCHEMA public TO admin")
        cur.execute("GRANT ALL ON SCHEMA public TO public")
    conn.commit()


def _seed_postgres(conn, policies: list[tuple], coverages: list[tuple], claims: list[tuple]) -> None:
    ddl = """
    CREATE TABLE policy (
        policy_id INTEGER PRIMARY KEY,
        policy_number VARCHAR(20) NOT NULL UNIQUE,
        product_line VARCHAR(32) NOT NULL,
        underwriting_company VARCHAR(80) NOT NULL,
        status VARCHAR(24) NOT NULL,
        effective_date DATE NOT NULL,
        expiration_date DATE NOT NULL,
        written_premium NUMERIC(12,2) NOT NULL,
        deductible_amount NUMERIC(12,2) NOT NULL,
        billing_plan VARCHAR(24) NOT NULL,
        agent_code VARCHAR(20) NOT NULL,
        risk_state CHAR(2) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE coverage (
        coverage_id INTEGER PRIMARY KEY,
        policy_id INTEGER NOT NULL REFERENCES policy(policy_id),
        coverage_code VARCHAR(32) NOT NULL,
        coverage_name VARCHAR(100) NOT NULL,
        coverage_limit NUMERIC(14,2) NOT NULL,
        deductible_amount NUMERIC(12,2) NOT NULL,
        premium_amount NUMERIC(12,2) NOT NULL,
        is_optional BOOLEAN NOT NULL DEFAULT false
    );

    CREATE TABLE claim (
        claim_id INTEGER PRIMARY KEY,
        claim_number VARCHAR(24) NOT NULL UNIQUE,
        policy_id INTEGER NOT NULL REFERENCES policy(policy_id),
        loss_date DATE NOT NULL,
        reported_date DATE NOT NULL,
        claim_status VARCHAR(24) NOT NULL,
        loss_cause VARCHAR(60) NOT NULL,
        reserve_amount NUMERIC(12,2) NOT NULL,
        paid_amount NUMERIC(12,2) NOT NULL,
        claim_state CHAR(2) NOT NULL,
        adjuster_name VARCHAR(80) NOT NULL,
        closed_date DATE
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        cur.executemany(
            """
            INSERT INTO policy (
                policy_id, policy_number, product_line, underwriting_company, status,
                effective_date, expiration_date, written_premium, deductible_amount,
                billing_plan, agent_code, risk_state
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            policies,
        )
        cur.executemany(
            """
            INSERT INTO coverage (
                coverage_id, policy_id, coverage_code, coverage_name, coverage_limit,
                deductible_amount, premium_amount, is_optional
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            coverages,
        )
        cur.executemany(
            """
            INSERT INTO claim (
                claim_id, claim_number, policy_id, loss_date, reported_date, claim_status,
                loss_cause, reserve_amount, paid_amount, claim_state, adjuster_name, closed_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            claims,
        )
    conn.commit()


def _drop_db2_views(cur, schema: str) -> None:
    cur.execute(
        "SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TYPE = 'V' ORDER BY TABNAME",
        (schema,),
    )
    for (name,) in cur.fetchall() or []:
        cur.execute(f'DROP VIEW "{schema}"."{str(name).strip()}"')


def _topological_table_order_db2(cur, schema: str) -> list[str]:
    cur.execute(
        "SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TYPE = 'T' ORDER BY TABNAME",
        (schema,),
    )
    table_names = [str(row[0]).strip() for row in cur.fetchall() or []]
    if not table_names:
        return []

    table_set = set(table_names)
    incoming = {name: set() for name in table_names}
    cur.execute(
        """
        SELECT TABNAME, REFTABNAME
        FROM SYSCAT.REFERENCES
        WHERE TABSCHEMA = ? AND REFTABSCHEMA = ?
        """,
        (schema, schema),
    )
    for child, parent in cur.fetchall() or []:
        child_name = str(child).strip()
        parent_name = str(parent).strip()
        if child_name in table_set and parent_name in table_set:
            incoming[child_name].add(parent_name)

    ready = sorted([name for name, deps in incoming.items() if not deps])
    ordered: list[str] = []
    while ready:
        current = ready.pop(0)
        ordered.append(current)
        for name, deps in incoming.items():
            if current in deps:
                deps.remove(current)
                if not deps and name not in ordered and name not in ready:
                    ready.append(name)
        ready.sort()

    for name in table_names:
        if name not in ordered:
            ordered.append(name)
    return ordered


def _reset_db2(conn, schema: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f'SET CURRENT SCHEMA "{schema}"')
        _drop_db2_views(cur, schema)
        create_order = _topological_table_order_db2(cur, schema)
        for table_name in reversed(create_order):
            cur.execute(f'DROP TABLE "{schema}"."{table_name}"')
        conn.commit()
    finally:
        cur.close()


def _seed_db2(
    conn,
    schema: str,
    people: list[tuple],
    addresses: list[tuple],
    phones: list[tuple],
    emails: list[tuple],
    policy_links: list[tuple],
) -> None:
    cur = conn.cursor()
    try:
        cur.execute(f'SET CURRENT SCHEMA "{schema}"')
        cur.execute(
            f"""
            CREATE TABLE "{schema}"."PERSON" (
                PERSON_ID INTEGER NOT NULL PRIMARY KEY,
                CUSTOMER_NUMBER VARCHAR(20) NOT NULL UNIQUE,
                FIRST_NAME VARCHAR(40) NOT NULL,
                LAST_NAME VARCHAR(40) NOT NULL,
                BIRTH_DATE DATE,
                OCCUPATION VARCHAR(80),
                ANNUAL_INCOME DECIMAL(12,2),
                PREFERRED_LANGUAGE VARCHAR(10) NOT NULL,
                CREATED_AT TIMESTAMP NOT NULL DEFAULT CURRENT TIMESTAMP
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE "{schema}"."ADDRESS" (
                ADDRESS_ID INTEGER NOT NULL PRIMARY KEY,
                PERSON_ID INTEGER NOT NULL,
                ADDRESS_TYPE VARCHAR(20) NOT NULL,
                ADDRESS_LINE1 VARCHAR(120) NOT NULL,
                ADDRESS_LINE2 VARCHAR(120),
                CITY VARCHAR(60) NOT NULL,
                STATE_CODE CHAR(2) NOT NULL,
                POSTAL_CODE VARCHAR(12) NOT NULL,
                COUNTRY_CODE CHAR(2) NOT NULL,
                IS_PRIMARY SMALLINT NOT NULL,
                CONSTRAINT FK_ADDRESS_PERSON FOREIGN KEY (PERSON_ID) REFERENCES "{schema}"."PERSON"(PERSON_ID)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE "{schema}"."PHONE" (
                PHONE_ID INTEGER NOT NULL PRIMARY KEY,
                PERSON_ID INTEGER NOT NULL,
                PHONE_TYPE VARCHAR(20) NOT NULL,
                PHONE_NUMBER VARCHAR(24) NOT NULL,
                IS_PRIMARY SMALLINT NOT NULL,
                CONSTRAINT FK_PHONE_PERSON FOREIGN KEY (PERSON_ID) REFERENCES "{schema}"."PERSON"(PERSON_ID)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE "{schema}"."EMAIL" (
                EMAIL_ID INTEGER NOT NULL PRIMARY KEY,
                PERSON_ID INTEGER NOT NULL,
                EMAIL_TYPE VARCHAR(20) NOT NULL,
                EMAIL_ADDRESS VARCHAR(160) NOT NULL,
                IS_PRIMARY SMALLINT NOT NULL,
                CONSTRAINT FK_EMAIL_PERSON FOREIGN KEY (PERSON_ID) REFERENCES "{schema}"."PERSON"(PERSON_ID)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE "{schema}"."POLICY" (
                POLICY_KEY INTEGER NOT NULL PRIMARY KEY,
                POLICY_NUMBER VARCHAR(20) NOT NULL UNIQUE,
                PERSON_ID INTEGER NOT NULL,
                RELATIONSHIP_ROLE VARCHAR(24) NOT NULL,
                EFFECTIVE_DATE DATE NOT NULL,
                EXPIRATION_DATE DATE NOT NULL,
                PREFERRED_CONTACT_CHANNEL VARCHAR(20) NOT NULL,
                CONSTRAINT FK_POLICY_PERSON FOREIGN KEY (PERSON_ID) REFERENCES "{schema}"."PERSON"(PERSON_ID)
            )
            """
        )

        cur.executemany(
            f"""
            INSERT INTO "{schema}"."PERSON" (
                PERSON_ID, CUSTOMER_NUMBER, FIRST_NAME, LAST_NAME,
                BIRTH_DATE, OCCUPATION, ANNUAL_INCOME, PREFERRED_LANGUAGE
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            people,
        )
        cur.executemany(
            f"""
            INSERT INTO "{schema}"."ADDRESS" (
                ADDRESS_ID, PERSON_ID, ADDRESS_TYPE, ADDRESS_LINE1, ADDRESS_LINE2,
                CITY, STATE_CODE, POSTAL_CODE, COUNTRY_CODE, IS_PRIMARY
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            addresses,
        )
        cur.executemany(
            f"""
            INSERT INTO "{schema}"."PHONE" (
                PHONE_ID, PERSON_ID, PHONE_TYPE, PHONE_NUMBER, IS_PRIMARY
            ) VALUES (?, ?, ?, ?, ?)
            """,
            phones,
        )
        cur.executemany(
            f"""
            INSERT INTO "{schema}"."EMAIL" (
                EMAIL_ID, PERSON_ID, EMAIL_TYPE, EMAIL_ADDRESS, IS_PRIMARY
            ) VALUES (?, ?, ?, ?, ?)
            """,
            emails,
        )
        cur.executemany(
            f"""
            INSERT INTO "{schema}"."POLICY" (
                POLICY_KEY, POLICY_NUMBER, PERSON_ID, RELATIONSHIP_ROLE,
                EFFECTIVE_DATE, EXPIRATION_DATE, PREFERRED_CONTACT_CHANNEL
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            policy_links,
        )
        conn.commit()
    finally:
        cur.close()


def _postgres_counts(conn) -> list[tuple[str, int]]:
    with conn.cursor() as cur:
        out: list[tuple[str, int]] = []
        for table_name in ("policy", "coverage", "claim"):
            cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            out.append((table_name, int(cur.fetchone()[0])))
        return out


def _db2_counts(conn, schema: str) -> list[tuple[str, int]]:
    cur = conn.cursor()
    try:
        out: list[tuple[str, int]] = []
        for table_name in ("PERSON", "ADDRESS", "PHONE", "EMAIL", "POLICY"):
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
            out.append((table_name, int(cur.fetchone()[0])))
        return out
    finally:
        cur.close()


def main() -> None:
    persons = _person_seeds()
    policies, coverages, claims, people_rows, addresses, phones, emails, db2_policy = _policy_rows(persons)

    with psycopg.connect(POSTGRES_DSN) as pg_conn:
        _reset_postgres(pg_conn)
        _seed_postgres(pg_conn, policies, coverages, claims)
        pg_counts = _postgres_counts(pg_conn)

    db2_conn = db2._dbapi_connect(DB2_CONN_STR)
    try:
        _reset_db2(db2_conn, DB2_SCHEMA)
        _seed_db2(db2_conn, DB2_SCHEMA, people_rows, addresses, phones, emails, db2_policy)
        db2_counts = _db2_counts(db2_conn, DB2_SCHEMA)
    finally:
        db2_conn.close()

    print("PostgreSQL seed complete:")
    for name, count in pg_counts:
        print(f"  - {name}: {count}")
    print("Db2 seed complete:")
    for name, count in db2_counts:
        print(f"  - {name}: {count}")
    print("Shared join key: policy.policy_number")


if __name__ == "__main__":
    main()
