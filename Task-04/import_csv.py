#!/usr/bin/env python3

import csv
import re
from datetime import datetime
from collections import Counter
import mysql.connector
from mysql.connector import errorcode
from dateutil.parser import parse as parse_date

import db_config as cfg

CSV_PATH = "movies.csv"
SAMPLE_ROWS_FOR_TYPE = 1000  


def normalize_colname(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^\w]", "", name)  
    if not name:
        name = "col"
    
    if re.match(r"^\d", name):
        name = "_" + name
    return name.lower()

def guess_type(values):
    """
    Given an iterable of strings, try to pick one of: INT, FLOAT, DATE, TEXT
    Rules: if all empty -> TEXT, if all int -> INT, if float->FLOAT, try DATE by parse
    """
    ints = 0
    floats = 0
    dates = 0
    empties = 0
    total = 0
    for v in values:
        total += 1
        v = v.strip()
        if v == "":
            empties += 1
            continue
        
        if re.fullmatch(r"[+-]?\d+", v):
            ints += 1
            continue
        
        if re.fullmatch(r"[+-]?\d+\.\d+", v):
            floats += 1
            continue
        
        try:
            
            parse_date(v, fuzzy=False)
            dates += 1
            continue
        except Exception:
            pass
    # decide
    if total == empties:
        return "TEXT"
    if ints + empties == total and ints > 0:
        return "INT"
    if (ints + floats) + empties == total and (floats > 0 or ints > 0):
        # if any float present -> FLOAT
        return "FLOAT" if floats > 0 else "INT"
    if dates + empties == total and dates > 0:
        return "DATE"
    return "TEXT"

def create_table(cursor, table_name, columns):
    """
    columns: list of tuples (colname, sqltype)
    """
    cols_def = []
    for name, typ in columns:
        if typ == "INT":
            cols_def.append(f"`{name}` INT NULL")
        elif typ == "FLOAT":
            cols_def.append(f"`{name}` DOUBLE NULL")
        elif typ == "DATE":
            cols_def.append(f"`{name}` DATE NULL")
        else:
            cols_def.append(f"`{name}` TEXT NULL")
    cols_sql = ",\n  ".join(cols_def)
    sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  id INT AUTO_INCREMENT PRIMARY KEY,\n  {cols_sql}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    cursor.execute(sql)

def to_sql_value(typ, raw):
    raw = raw.strip()
    if raw == "":
        return None
    if typ == "INT":
        try:
            return int(raw)
        except:
            return None
    if typ == "FLOAT":
        try:
            return float(raw)
        except:
            return None
    if typ == "DATE":
        try:
            dt = parse_date(raw, fuzzy=False)
            return dt.date().isoformat()
        except:
            return None
    return raw  

def main():
    
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            print("CSV is empty.")
            return
        raw_cols = header
        normalized = [normalize_colname(c) or f"col{i}" for i,c in enumerate(raw_cols)]
       
        seen = Counter()
        final_cols = []
        for i,name in enumerate(normalized):
            seen[name] += 1
            if seen[name] > 1:
                name = f"{name}_{seen[name]}"
            final_cols.append(name)
       
        samples = [[] for _ in final_cols]
        rows_for_insert = []
        for i,row in enumerate(reader):
            if i < SAMPLE_ROWS_FOR_TYPE:
                for j,cell in enumerate(row):
                    if j < len(samples):
                        samples[j].append(cell)
            rows_for_insert.append(row)
        
        col_types = []
        for s in samples:
            typ = guess_type(s)
            col_types.append(typ)
        cols = list(zip(final_cols, col_types))
    
    try:
        cnx = mysql.connector.connect(
            host=cfg.DB_HOST,
            port=cfg.DB_PORT,
            user=cfg.DB_USER,
            password=cfg.DB_PASS
        )
    except mysql.connector.Error as err:
        print("Error connecting to MySQL:", err)
        return
    cursor = cnx.cursor()
    
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{cfg.DB_NAME}` DEFAULT CHARACTER SET 'utf8mb4'")
    except mysql.connector.Error as err:
        print("Failed creating database:", err)
        cursor.close()
        cnx.close()
        return
    cnx.database = cfg.DB_NAME

    
    create_table(cursor, cfg.TABLE_NAME, cols)
    cnx.commit()

    
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)  
        placeholders = ", ".join(["%s"] * len(final_cols))
        columns_clause = ", ".join([f"`{c}`" for c in final_cols])
        insert_sql = f"INSERT INTO `{cfg.TABLE_NAME}` ({columns_clause}) VALUES ({placeholders})"
        inserted = 0
        batch = []
        BATCH_SIZE = 500
        for row in reader:
            vals = []
            for idx in range(len(final_cols)):
                raw = row[idx] if idx < len(row) else ""
                typed = to_sql_value(col_types[idx], raw)
                vals.append(typed)
            batch.append(vals)
            if len(batch) >= BATCH_SIZE:
                cursor.executemany(insert_sql, batch)
                cnx.commit()
                inserted += len(batch)
                print(f"Inserted {inserted} rows...")
                batch = []
        if batch:
            cursor.executemany(insert_sql, batch)
            cnx.commit()
            inserted += len(batch)
        print(f"Import complete. Inserted {inserted} rows into {cfg.DB_NAME}.{cfg.TABLE_NAME}")
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()
