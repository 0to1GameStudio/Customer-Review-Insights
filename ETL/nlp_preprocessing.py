import pandas as pd
from database_data import CSV_SQL
from logging_config import logger

logger.info('NLP Pre-Processing Started...')
# Setting up or loading CSV file into Python environment.
class nlp_ETL:
    def __init__(self,input_filename,output_filename):
        self.input_filename = input_filename
        self.output_filename = output_filename
        self.data = None

    def extract(self):
        # Extracts the CSV file
        try:
            self.data = pd.read_csv(self.input_filename)
            logger.info('Data Extraction Succesful.')
        except Exception as e:
            logger.error(f'Error during extraction.')


    def transform(self):
        # All the transformations will work in this function.

        #print(self.data)        
        if self.data is not None:
        
            logger.info('Deleting the Duplicates placed in dataset.')
            # Remove rows with null values
            self.data = self.data.dropna()
            # Convert 'Review' column to string and preprocess with removal of redundant or noisy data.            
            self.data['Review'] = self.data['Review'].astype(str)
            self.data['Review'] = self.data['Review'].str.replace(r'[!,@,#]', '', regex=True)
            self.data['Review'] = self.data['Review'].str.lower()
            
            # Convert 'Summary' column to string and preprocess
            self.data['Summary'] = self.data['Summary'].astype(str)
            self.data['Summary'] = self.data['Summary'].str.lower()

            # listing what kind of noisy data we got from csv file. Then delete and load it to dataframe.
            noisy_values = [
                'Pigeon Favourite Electric Kettle??????(1.5 L, Silver, Black)',
                "Bajaj DX 2 L/W Dry Iron",
                'Nova Plus Amaze NI 10 1100 W Dry Iron?ÃÂ¿?ÃÂ¿(Grey & Turquoise)'
            ]
            self.data = self.data[~self.data['Rate'].isin(noisy_values)]

            # self.data = self.data[(self.data.Rate !='Pigeon Favourite Electric Kettle??????(1.5 L, Silver, Black)') 
            #         & (self.data.Rate != "Bajaj DX 2 L/W Dry Iron") 
            #         & (self.data.Rate !='Nova Plus Amaze NI 10 1100 W Dry Iron?ÃÂ¿?ÃÂ¿(Grey & Turquoise)')]
            
            # As unexpectedly we got noisy data or irrelevant information that can negatively impact data analysis.\n'
            # 'Then deleted those values or rows in dataset.

            # print('\n Summary of the Dataset : \n',self.data.describe())

            # Making all column names to lower-case.
            self.data.columns = self.data.columns.str.lower()
            logger.info('Data Transformation Successfully done!')
        else:
            logger.warning('No Data to Transform.')

    def loading(self):
        # Save the processed DataFrame to a CSV file
        if self.data is not None:
            self.data.to_csv(self.output_filename, index=False,mode='w')  # Write the DataFrame to CSV
            path_to = 'Customer Review Insights Project -> Project -> data -> Processed_Dataset.csv'
            logger.info('Successfully Data Loaded to Target Destination / Location. with location:')
            logger.info('Updating those records or data to the database - PostgreSQL')
            # Calls the csv_sql of database_data python file to process data and store it into database.
            csv_loader = CSV_SQL(self.output_filename)
            csv_loader.csv_to_sql()
        else:
            logger.warning('No Data is to Load.')
    
    def run(self):
        """Executing the ETL Pipeline. """
        self.extract()
        self.transform()
        self.loading()


if __name__ == '__main__':
    in_file_name = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Dataset-SA.csv'
    out_filename = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Processed_Dataset.csv'

    ETL = nlp_ETL(in_file_name,out_filename)
    ETL.run()
