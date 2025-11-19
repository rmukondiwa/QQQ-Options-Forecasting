import pandas as pd
from google.colab import drive
import os
from zipfile import ZipFile

class DriveDataLoader:
    '''
    Implementation process notes:
    - At first, I attempted to download the zip file directly into the directory, quickly learned I can just mount Google Drive == quicker and less space-consuming!
    
    '''
    
    def __init__(self, driveFolder="QuantChallenge", zipName="options_eod_QQQ.zio"):
        """Initializes the DriveDataLoader with specified Google Drive folder and zip file name."""
        self.driveFolder = driveFolder
        self.zipName = zipName

        # Path inside Google Drive to the zip file
        self.drivePath = f"/content/drive/MyDrive/{self.driveFolder}"
        self.zipPath = f"{self.drivePath}/{self.zipName}"
        self.extractedCSVPath = None

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
