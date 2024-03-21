from pathlib import Path
import pyodbc

class Settings():
    basedir = Path.cwd()
    rawdir = Path("raw_data")
    processeddir = Path("processed_data")



settings = Settings()