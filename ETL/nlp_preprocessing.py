import pandas as pd

print('Testing')
    
#input_file = r"D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\Data\Datasete-SA.csv"
#df = pd.read_csv(r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\Data\Datasete-SA.csv')
#df.head()

#setting up or loading csv file into python environment.
class nlp:
     def __init__(self,filename):
        self.filename = file_name
        
     def extract(self):
        df = pd.read_csv(self.filename)
        return df.head()
        

if __name__ == '__main__':
    file_name= r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Dataset-SA.csv'
    data_reader = nlp(file_name)
    datas = data_reader.extract()
    print(datas)



