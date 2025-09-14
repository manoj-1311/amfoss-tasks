CineScope Dashboard - SQL Integration

Prereqs:
 1.MySQL server installed and running.
 2.Create a MySQL user with privileges to create DB/tables OR use an existing user.
Setup:
 1. Put your movies.csv in project root.
 2. Edit db_config.py with DB credentials and desired DB/TABLE names.
 3. Create a venv and install dependencies:
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
 4. Import CSV into DB:
    python import_csv.py
 5. Run the dashboard:
    python dashboard.p
    
