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
        self.zipPath = f"/content/drive/MyDrive/{self.driveFolder}/{self.zipName}"
        self.extractedCSVPath = None

