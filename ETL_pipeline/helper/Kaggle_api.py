import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
from config.config import CONFIG


# creating class to fatch data from KaggleAPI
class KaggleDataDownloader:

    def __init__(self, dataset, file_name):
        try:
            self.dataset = dataset
            self.file_name = file_name

        except Exception as e:
            print(e)
    def download(self):
        
        '''
        This function will download the data from Kaggle API.
        '''
        try:
            kaggle.api.authenticate()
            download_path = CONFIG["KAGGLE"]["LOCAL_DOWNLOAD_FOLDER"] # define the download path using os.path.join
            kaggle.api.dataset_download_file(self.dataset,self.file_name, path=download_path)
            
        except Exception as e:
            print("Kaggle API")
            raise e
        
        
 
    