# Customer Data Pipeline - Complete Assessment Solution

---

# PROJECT STRUCTURE

project-root/
│
├── docker-compose.yml
│
├── mock-server/
│   ├── app.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── data/customers.json
│
└── pipeline-service/
    ├── main.py
    ├── database.py
    ├── models/customer.py
    ├── services/ingestion.py
    ├── Dockerfile
    └── requirements.txt

---

========================================
docker-compose.yml
========================================

version: "3.9"

services:

  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
      POSTGRES_DB: customer_db

  mock-server:
    build: ./mock-server
    ports:
      - "5000:5000"
    depends_on:
      - postgres

  pipeline-service:
    build: ./pipeline-service
    ports:
      - "8000:8000"
    depends_on:
      - postgres
      - mock-server
    environment:
      DATABASE_URL: postgresql://postgres:password@postgres:5432/customer_db


========================================
mock-server/requirements.txt
========================================

Flask==3.0.0


========================================
mock-server/Dockerfile
========================================

FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["python", "app.py"]


========================================
mock-server/app.py
========================================

import json
from flask import Flask, jsonify, request, abort
import os

app = Flask(__name__)

DATA_FILE = os.path.join("data", "customers.json")

with open(DATA_FILE, "r") as f:
    customers = json.load(f)

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy"})

@app.route("/api/customers", methods=["GET"])
def get_customers():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 10))

    start = (page - 1) * limit
    end = start + limit

    paginated = customers[start:end]

    return jsonify({
        "data": paginated,
        "total": len(customers),
        "page": page,
        "limit": limit
    })

@app.route("/api/customers/<customer_id>", methods=["GET"])
def get_customer(customer_id):
    for c in customers:
        if c["customer_id"] == customer_id:
            return jsonify(c)
    abort(404, description="Customer not found")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


========================================
mock-server/data/customers.json
(20 Sample Records)
========================================

[
  {
    "customer_id": "CUST001",
    "first_name": "John",
    "last_name": "Doe",
    "email": "john1@example.com",
    "phone": "9876543210",
    "address": "Bangalore, India",
    "date_of_birth": "1990-01-01",
    "account_balance": 1500.50,
    "created_at": "2024-01-01T10:00:00"
  },
  {
    "customer_id": "CUST002",
    "first_name": "Jane",
    "last_name": "Smith",
    "email": "jane2@example.com",
    "phone": "9876543211",
    "address": "Hyderabad, India",
    "date_of_birth": "1992-05-10",
    "account_balance": 2500.75,
    "created_at": "2024-01-02T10:00:00"
  }
]

(Repeat similar structure up to CUST020)


========================================
pipeline-service/requirements.txt
========================================

fastapi==0.110.0
uvicorn==0.27.1
sqlalchemy==2.0.25
psycopg2-binary==2.9.9
requests==2.31.0


========================================
pipeline-service/Dockerfile
========================================

FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]


========================================
pipeline-service/database.py
========================================

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


========================================
pipeline-service/models/customer.py
========================================

from sqlalchemy import Column, String, Text, Date, DECIMAL, TIMESTAMP
from database import Base

class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(String(50), primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(20))
    address = Column(Text)
    date_of_birth = Column(Date)
    account_balance = Column(DECIMAL(15,2))
    created_at = Column(TIMESTAMP)


========================================
pipeline-service/services/ingestion.py
========================================

import requests
from database import SessionLocal
from models.customer import Customer

FLASK_URL = "http://mock-server:5000/api/customers"

def fetch_all_customers():
    page = 1
    limit = 10
    all_data = []

    while True:
        res = requests.get(f"{FLASK_URL}?page={page}&limit={limit}")
        data = res.json()
        if not data["data"]:
            break
        all_data.extend(data["data"])
        page += 1

    return all_data

def upsert_customers():
    db = SessionLocal()
    customers = fetch_all_customers()
    count = 0

    for c in customers:
        existing = db.get(Customer, c["customer_id"])
        if existing:
            for key, value in c.items():
                setattr(existing, key, value)
        else:
            db.add(Customer(**c))
        count += 1

    db.commit()
    db.close()
    return count


========================================
pipeline-service/main.py
========================================

from fastapi import FastAPI, HTTPException
from database import engine, SessionLocal
from models.customer import Base, Customer
from services.ingestion import upsert_customers

app = FastAPI()

Base.metadata.create_all(bind=engine)

@app.post("/api/ingest")
def ingest():
    count = upsert_customers()
    return {"status": "success", "records_processed": count}

@app.get("/api/customers")
def get_customers(page: int = 1, limit: int = 10):
    db = SessionLocal()
    start = (page - 1) * limit
    results = db.query(Customer).offset(start).limit(limit).all()
    total = db.query(Customer).count()
    db.close()

    return {
        "data": results,
        "total": total,
        "page": page,
        "limit": limit
    }

@app.get("/api/customers/{customer_id}")
def get_customer(customer_id: str):
    db = SessionLocal()
    customer = db.get(Customer, customer_id)
    db.close()

    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    return customer
