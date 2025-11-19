import pandas as pd
from google.colab import drive
import os
import pyarrow as pa
import pyarrow.parquet as pq
from zipfile import ZipFile

class DriveDataLoader:
    '''
    Implementation process notes:
    - At first, I attempted to download the zip file directly into the directory, quickly learned I can just mount Google Drive == quicker and less space-consuming!
    - I first thought about loading data chunks into a list, quickly realized there was probably a more efficient way ==> Apache Parquet
    '''
    
    def __init__(self, driveFolder="QuantChallenge", zipName="options_eod_QQQ.zip", parquetName="options_eod_QQQ.parquet"):
        """Initializes the DriveDataLoader with specified Google Drive folder and zip file name."""
        self.driveFolder = driveFolder
        self.zipName = zipName

        # Path inside Google Drive to the zip file
        self.drivePath = f"/content/drive/MyDrive/{self.driveFolder}"
        self.zipPath = f"{self.drivePath}/{self.zipName}"
        self.extractedCSVPath = None
        self.parquetPath = f"{self.drivePath}/{parquetName}"

    def mountDrive(self):
        """Mounts Google Drive once"""
        drive.mount('/content/drive')
        print("Google Drive mounted successfully.")

    def unzipIfNeeded(self):
        """Unzips the file if it hasn't been unzipped yet."""
        zipFilePath = self.zipPath

        with ZipFile(zipFilePath, 'r') as zip:
            csvName = zip.namelist()[0]
            self.extractedCSVPath = f"{self.drivePath}/{csvName}"

            if not os.path.exists(self.extractedCSVPath):
                print(f"Extracting {csvName} from zip...")
                zip.extract(csvName, self.drivePath)
            else:
                print(f"{csvName} already extracted.")
        return self.extractedCSVPath

    def loadDataToParquet(self, chunksize=500_000):
        """Load the massive CSV to parquet in chunks to be more memory and speed efficient."""
        
        if os.path.exists(self.parquetPath):
            print("Parquet already exists, skipping convesion")
            return self.parquetPath
        
        print("Converting CSV to Parquet in chunks...")

        firstWrite = True

        for chunk in pd.read_csv(self.extractedCSVPath, chunksize=chunksize):
            table = pa.Table.from_pandas(chunk)

            if firstWrite:
                pq.write_table(table, self.parquetPath)
                firstWrite = False
            else:
                pq.write_table(table, self.parquetPath, append=True)

        print("Finished writing Parquet: {self.parquetPath}")
        return self.parquetPath

    def loadParquet(self):
        """Loads the dataset from Parquet file."""
        print("loading dataset from Parquet (fast)...")
        df = pd.read_parquet(self.parquetPath)
        print("Loaded!")
        return df
    
    def load(self):
        """Main method to be called to load the data."""
        self.mountDrive()
        self.unzipIfNeeded()
        self.loadDataToParquet()
        df = self.loadParquet()
        return df