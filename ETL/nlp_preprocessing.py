import pandas as pd

print('NLP Pre-Processing...')

# Setting up or loading CSV file into Python environment.
class nlp:
    def __init__(self, filename):
        self.filename = filename 

    def extract(self):
        # Load the CSV file
        df = pd.read_csv(self.filename)
        return df.head()

    def transform(self, df):
        # Convert 'Review' column to string and preprocess
        df['Review'] = df['Review'].astype(str)
        df['Review'] = df['Review'].str.replace(r'[!,@,#]', '', regex=True)
        df['Review'] = df['Review'].str.lower()

        # Convert 'Summary' column to string and preprocess
        df['Summary'] = df['Summary'].astype(str)
        df['Summary'] = df['Summary'].str.lower()

        # Remove rows with null values
        df = df.dropna()

        return df  # Return the cleaned DataFrame

    def loading(self, df):
        # Save the processed DataFrame to a CSV file
        output_file = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Processed_Dataset.csv'
        df.to_csv(output_file, index=False)  # Write the DataFrame to CSV
        return output_file  # Return the file path for confirmation

if __name__ == '__main__':
    file_name = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Dataset-SA.csv'
    data_reader = nlp(file_name)

    # Extract the data
    datas = data_reader.extract()
    print("Extracted Data:")
    print(datas)

    # Transform the data
    transformed_data = data_reader.transform(datas)
    print("Transformed Data:")
    print(transformed_data)

    # Load the processed data to a CSV file
    output_file_path = data_reader.loading(transformed_data)
    print(f"Data transformed and saved to: {output_file_path}")
