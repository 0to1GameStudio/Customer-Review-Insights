import pandas as pd
from database_data import CSV_SQL

print('NLP Pre-Processing...')
# Setting up or loading CSV file into Python environment.
class nlp:
    def __init__(self,input_filename,output_filename):
        self.input_filename = input_filename
        self.output_filename = output_filename
        self.data = None

    def extract(self):
        # Load the CSV file
        try:
            self.data = pd.read_csv(self.input_filename)
            print('Data Extraction Succesful.\n')
        except Exception as e:
            print(f'Error during extraction {e}')


    def transform(self):
        # Convert 'Review' column to string and preprocess
        print(self.data)        
        if self.data is not None:
        
            print('\n Deleting the Duplicates placed in dataset.\n ')
            # Remove rows with null values
            self.data = self.data.dropna()
            
            self.data['Review'] = self.data['Review'].astype(str)
            self.data['Review'] = self.data['Review'].str.replace(r'[!,@,#]', '', regex=True)
            self.data['Review'] = self.data['Review'].str.lower()
            
            # Convert 'Summary' column to string and preprocess
            self.data['Summary'] = self.data['Summary'].astype(str)
            self.data['Summary'] = self.data['Summary'].str.lower()

            print('\n Counting Ratings for each product. \n')
            print(self.data['Rate'].value_counts())

            self.data = self.data[(self.data.Rate !='Pigeon Favourite Electric Kettle??????(1.5 L, Silver, Black)') 
                    & (self.data.Rate != "Bajaj DX 2 L/W Dry Iron") 
                    & (self.data.Rate !='Nova Plus Amaze NI 10 1100 W Dry Iron?ÃÂ¿?ÃÂ¿(Grey & Turquoise)')]
            
            print('\n As unexpectedly we got noisy data or irrelevant information that can negatively impact data analysis.\n'
            'Then deleted those values or rows in dataset.')
            print('Finally, After deleting noisy data. Counting rating for each product.')
            print('\n',self.data['Rate'].value_counts())

            print('\n Summary of the Dataset : \n',self.data.describe())
            print('Data Transformation Successfully done!')
        else:
            print('No Data to Transform.')

    def loading(self):
        # Save the processed DataFrame to a CSV file
        if self.data is not None:
            self.data.to_csv(self.output_filename, index=False,mode='w')  # Write the DataFrame to CSV
            path_to = 'Customer Review Insights Project -> Project -> data -> Processed_Dataset.csv'
            print('\nSuccessfully Data Loaded to Target Destination / Location. with location: ',path_to)
            print('Updating those records or data to the database - PostgreSQL',self.data.shape)
            CSV_SQL(self.data)
        else:
            print('No Data is to Load. ')
    
    def run(self):
        """Execute the ETL Pipeline. """
        self.extract()
        self.transform()
        self.loading()


if __name__ == '__main__':
    in_file_name = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Dataset-SA.csv'
    out_filename = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Processed_Dataset.csv'

    ETL = nlp(in_file_name,out_filename)
    ETL.run()

