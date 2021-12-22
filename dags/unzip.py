import os
import zipfile
import pandas as pd


def unzip(API_command):
    os.system(API_command)          # imports to terminal
    with zipfile.ZipFile('/Users/cantekinefe/airflow/airflow_home/dags/netflix-shows.zip', "r") as zip:
        filename = zip.namelist()
        zip.extractall('/Users/cantekinefe/airflow/airflow_home/dags')

    dataset = []
    for line in filename:
        dataset.append(pd.read_csv(line))
        filenames = filename


unzip("kaggle datasets download -d shivamb/netflix-shows")









# class Database:
#     def __init__(self, API_command):
#         os.system(API_command)  # imports to terminal
#         self.name = API_command.split('/')  # take letter after first slash in url
#         self.Open()
#         os.remove(self.name[1] + ".zip")  # removes file from memory
#
#     def Open(self):
#         self.dataset = []
#         with zipfile.ZipFile(self.name[1] + ".zip", "r") as zip:
#             filename = zip.namelist()
#             zip.extractall()
#         for i in filename:
#             self.dataset.append(pd.read_csv(i))
#         self.filenames = filename
#
#
# Database("kaggle datasets download -d shivamb/netflix-shows")

# data = Database("kaggle datasets download -d shivamb/netflix-shows")
# for i in data.dataset:
#     print(i)


