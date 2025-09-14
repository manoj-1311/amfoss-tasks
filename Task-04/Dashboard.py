#!/usr/bin/env python3

import sys
import csv
from PySide6 import QtWidgets, QtCore, QtGui
import mysql.connector
from mysql.connector import Error
import db_config as cfg

class DB:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                host=cfg.DB_HOST,
                port=cfg.DB_PORT,
                user=cfg.DB_USER,
                password=cfg.DB_PASS,
                database=cfg.DB_NAME,
                charset='utf8mb4'
            )
        except Error as e:
            QtWidgets.QMessageBox.critical(None, "DB Error", f"Could not connect to DB: {e}")
            raise

    def list_columns(self):
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM `{cfg.TABLE_NAME}` LIMIT 1")
        cols = [desc[0] for desc in cur.description]
        cur.close()
        
        return cols

    def run_query(self, columns, search_text):
        cur = self.conn.cursor()
        cols_sql = ", ".join([f"`{c}`" for c in columns])
        sql = f"SELECT {cols_sql} FROM `{cfg.TABLE_NAME}`"
        params = []
        if search_text:
            like_clause = " OR ".join([f"`{c}` LIKE %s" for c in columns])
            sql += " WHERE " + like_clause
            params = [f"%{search_text}%"] * len(columns)
        sql += " LIMIT 5000"  
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return rows

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CineScope Dashboard")
        self.resize(900, 600)
        self.db = DB()
        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        v = QtWidgets.QVBoxLayout(central)

       
        controls = QtWidgets.QHBoxLayout()
        self.search_input = QtWidgets.QLineEdit()
        self.search_input.setPlaceholderText("Enter search text (applies to selected columns)...")
        controls.addWidget(self.search_input)

        self.btn_search = QtWidgets.QPushButton("Search")
        controls.addWidget(self.btn_search)

        self.btn_export = QtWidgets.QPushButton("Export CSV")
        controls.addWidget(self.btn_export)

        self.btn_refresh_cols = QtWidgets.QPushButton("Refresh Columns")
        controls.addWidget(self.btn_refresh_cols)

        v.addLayout(controls)

       
        middle = QtWidgets.QHBoxLayout()
        self.columns_group = QtWidgets.QGroupBox("Columns to display")
        self.columns_layout = QtWidgets.QVBoxLayout()
        self.columns_group.setLayout(self.columns_layout)
       
        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        inner = QtWidgets.QWidget()
        inner.setLayout(QtWidgets.QVBoxLayout())
        inner.layout().addWidget(self.columns_group)
        scroll.setWidget(inner)
        scroll.setFixedWidth(250)
        middle.addWidget(scroll)

        
        self.table = QtWidgets.QTableWidget()
        middle.addWidget(self.table)
        v.addLayout(middle)

       
        self.status = QtWidgets.QStatusBar()
        self.setStatusBar(self.status)

        
        self.btn_search.clicked.connect(self.on_search)
        self.btn_export.clicked.connect(self.on_export)
        self.btn_refresh_cols.clicked.connect(self.load_columns)

        
        try:
            self.db.connect()
            self.load_columns()
        except Exception:
            self.status.showMessage("DB connection failed.")

    def clear_column_checkboxes(self):
        
        for i in reversed(range(self.columns_layout.count())):
            item = self.columns_layout.itemAt(i).widget()
            if item:
                item.setParent(None)

    def load_columns(self):
        try:
            cols = self.db.list_columns()
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Could not list columns: {e}")
            return
        self.clear_column_checkboxes()
        self.checkboxes = []
        for c in cols:
          
            cb = QtWidgets.QCheckBox(c)
            cb.setChecked(True if c.lower() != "id" else False)
            self.columns_layout.addWidget(cb)
            self.checkboxes.append(cb)
       
        self.columns_layout.addStretch()

    def selected_columns(self):
        cols = [cb.text() for cb in self.checkboxes if cb.isChecked()]
        if not cols:
            
            cols = [cb.text() for cb in self.checkboxes]
        return cols

    def on_search(self):
        cols = self.selected_columns()
        if not cols:
            QtWidgets.QMessageBox.warning(self, "No columns", "Please select at least one column to display/search.")
            return
        search_text = self.search_input.text().strip()
        try:
            rows = self.db.run_query(cols, search_text)
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Query error", f"Error running query: {e}")
            return
        self.populate_table(cols, rows)
        self.status.showMessage(f"Showing {len(rows)} rows (max 5000).")

    def populate_table(self, cols, rows):
        self.table.clear()
        self.table.setColumnCount(len(cols))
        self.table.setRowCount(len(rows))
        self.table.setHorizontalHeaderLabels(cols)
        for r, row in enumerate(rows):
            for c, val in enumerate(row):
                item = QtWidgets.QTableWidgetItem("" if val is None else str(val))
                
                try:
                    float_val = float(val)
                    item.setTextAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
                except Exception:
                    item.setTextAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
                self.table.setItem(r, c, item)
        self.table.resizeColumnsToContents()

    def on_export(self):
        
        rows = self.table.rowCount()
        cols = self.table.columnCount()
        if rows == 0 or cols == 0:
            QtWidgets.QMessageBox.information(self, "No data", "No data to export. Run a search first.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Export CSV", "export.csv", "CSV Files (*.csv)")
        if not path:
            return
        headers = [self.table.horizontalHeaderItem(c).text() for c in range(cols)]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for r in range(rows):
                rowvals = []
                for c in range(cols):
                    it = self.table.item(r, c)
                    rowvals.append(it.text() if it else "")
                writer.writerow(rowvals)
        QtWidgets.QMessageBox.information(self, "Exported", f"Exported {rows} rows to {path}")

def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
