"""
Generate Sample Datasets
=========================
Creates realistic test datasets that simulate real-world messy data:
- consultations.csv  (hospital data — used in the portfolio OLAP project)
- sales.csv          (e-commerce sales)
- employees.json     (HR data)

Data intentionally includes:
- Missing values
- Duplicates
- Out-of-range values
- Wrong types
- Extra whitespace

Author : Mounir Bekkar
"""

import os
import json
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path


random.seed(42)
np.random.seed(42)
OUTPUT_DIR = Path("data")


# ── CONSULTATIONS CSV (Hospital OLAP dataset) ─────────────────────────────────

def generate_consultations(n: int = 500) -> pd.DataFrame:
    """
    Simulates hospital consultation data.
    Matches the OLAP project schema: ID, Patient, Docteur, Service, Date, Cout, Duree, Diagnostic
    """
    SERVICES    = ["Cardiologie","Pédiatrie","Urgences","Chirurgie","Dermatologie","Neurologie"]
    DIAGNOSTICS = ["Diabète","Hypertension","Asthme","Fracture","Allergie","Grippe","Migraines","Anémie"]
    DOCTEURS    = [f"Dr. {name}" for name in ["Martin","Bernard","Dupont","Moreau","Laurent","Simon","Leroy","Petit"]]

    start_date = datetime(2024, 1, 1)

    rows = []
    for i in range(n):
        # Intentional messy data
        has_null_date    = random.random() < 0.04
        has_null_patient = random.random() < 0.02
        has_null_cout    = random.random() < 0.03
        out_of_range     = random.random() < 0.05   # negative cost
        bad_duree        = random.random() < 0.03   # duration > 300 min
        extra_whitespace = random.random() < 0.10

        date = (start_date + timedelta(days=random.randint(0, 364))).strftime("%d/%m/%Y") \
               if not has_null_date else None

        service    = random.choice(SERVICES)
        diagnostic = random.choice(DIAGNOSTICS)
        docteur    = random.choice(DOCTEURS)

        if extra_whitespace:
            service = "  " + service + " "

        rows.append({
            "id":          i + 1,
            "patient_id":  random.randint(1, 100) if not has_null_patient else None,
            "docteur":     docteur,
            "service":     service,
            "date":        date,
            "cout":        round(random.uniform(50, 500), 2) if not has_null_cout else
                           (round(random.uniform(-100, -1), 2) if out_of_range else None),
            "duree_min":   random.randint(10, 90) if not bad_duree else random.randint(300, 600),
            "diagnostic":  diagnostic,
        })

    df = pd.DataFrame(rows)

    # Add ~3% duplicates
    dupes = df.sample(frac=0.03, random_state=1)
    df    = pd.concat([df, dupes], ignore_index=True)

    return df.sample(frac=1, random_state=42).reset_index(drop=True)


# ── SALES CSV ─────────────────────────────────────────────────────────────────

def generate_sales(n: int = 400) -> pd.DataFrame:
    """Simulates e-commerce sales data."""
    CATEGORIES = ["Electronics","Clothing","Books","Sports","Home","Beauty"]
    STATUSES   = ["completed","pending","cancelled","refunded"]
    CITIES     = ["Paris","Lyon","Marseille","Bordeaux","Toulouse","Nice","Nantes","Strasbourg"]

    rows = []
    start = datetime(2024, 1, 1)

    for i in range(n):
        has_null  = random.random() < 0.05
        neg_price = random.random() < 0.04

        rows.append({
            "order_id":   f"ORD-{10000 + i}",
            "customer_id": random.randint(1, 200),
            "date":        (start + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d"),
            "product":     f"Product_{random.randint(1, 50)}",
            "category":    random.choice(CATEGORIES) if not has_null else None,
            "quantity":    random.randint(1, 10),
            "unit_price":  round(random.uniform(5, 500), 2) if not neg_price
                           else round(random.uniform(-50, -1), 2),
            "total":       None,  # will be computed in transformation
            "status":      random.choice(STATUSES),
            "city":        random.choice(CITIES),
        })

    df = pd.DataFrame(rows)

    # Add duplicates
    dupes = df.sample(frac=0.04, random_state=2)
    df    = pd.concat([df, dupes], ignore_index=True)

    return df.sample(frac=1, random_state=42).reset_index(drop=True)


# ── EMPLOYEES JSON ────────────────────────────────────────────────────────────

def generate_employees(n: int = 100) -> list[dict]:
    """Simulates HR employee data."""
    DEPARTMENTS = ["IT","Finance","Marketing","HR","Operations","Sales","R&D"]
    ROLES       = ["Engineer","Manager","Analyst","Director","Intern","Lead"]

    employees = []
    for i in range(n):
        has_null_salary = random.random() < 0.06
        has_null_dept   = random.random() < 0.04
        negative_salary = random.random() < 0.03

        employees.append({
            "id":         i + 1,
            "name":       f"Employé_{i+1}",
            "email":      f"employee{i+1}@company.com",
            "department": random.choice(DEPARTMENTS) if not has_null_dept else None,
            "role":       random.choice(ROLES),
            "salary":     round(random.uniform(25000, 120000), 2) if not has_null_salary
                          else (round(random.uniform(-5000, -1), 2) if negative_salary else None),
            "hire_date":  (datetime(2015, 1, 1) + timedelta(days=random.randint(0, 3285))).strftime("%Y-%m-%d"),
            "active":     random.choice([True, False, True, True]),  # mostly active
        })

    return employees


# ── GENERATE ALL ──────────────────────────────────────────────────────────────

def generate_all():
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Consultations CSV
    df_consult = generate_consultations(500)
    path_c = OUTPUT_DIR / "consultations.csv"
    df_consult.to_csv(path_c, index=False)
    print(f"  ✓ consultations.csv  — {len(df_consult):,} rows → {path_c}")

    # Sales CSV
    df_sales = generate_sales(400)
    path_s   = OUTPUT_DIR / "sales.csv"
    df_sales.to_csv(path_s, index=False)
    print(f"  ✓ sales.csv          — {len(df_sales):,} rows → {path_s}")

    # Employees JSON
    employees = generate_employees(100)
    path_e    = OUTPUT_DIR / "employees.json"
    with open(path_e, "w", encoding="utf-8") as f:
        json.dump(employees, f, indent=2, ensure_ascii=False)
    print(f"  ✓ employees.json     — {len(employees):,} records → {path_e}")

    print(f"\n[DONE] All datasets generated in '{OUTPUT_DIR}/'")
    print("\nRun the pipeline on consultations:")
    print("  python run_pipeline.py \\")
    print("    --source data/consultations.csv \\")
    print("    --table  consultations_clean \\")
    print("    --not-null '[\"id\",\"patient_id\",\"date\"]' \\")
    print("    --types  '{\"date\":\"datetime\",\"cout\":\"float\",\"duree_min\":\"int\"}' \\")
    print("    --ranges '{\"cout\":[0,1000],\"duree_min\":[5,300]}' \\")
    print("    --normalize '[\"service\",\"diagnostic\"]'\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate sample ETL test datasets")
    parser.add_argument("--out", default=str(OUTPUT_DIR), help="Output directory")
    args = parser.parse_args()

    OUTPUT_DIR = Path(args.out)
    print(f"\n[DATASET] Generating sample data in '{OUTPUT_DIR}/' ...")
    generate_all()
