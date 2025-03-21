import pandas as pd
from sqlalchemy import create_engine
import os

class CSV_SQL:
    def __init__(self,input_file):
        self.input_file = input_file
        
    def csv_to_sql(self):
        # This function is used to convert the processed data from target location 
        # or destination to the database - PostgreSQL.    
        try:
            alchemyEngine = create_engine('postgresql+psycopg2://postgres:nik123@127.0.0.1:5434/CRI',pool_recycle=3600)
            conn = alchemyEngine.connect()
            print('Database connected.')
            if os.path.exists(self.input_file):
                
                print('File is present in the folder : ',self.input_file)
                print('\n Converting the processed dataset of csv format from target location / '
                'destination to the database for Analysis and Reporting\n')
                
                df = pd.read_csv(self.input_file)
                # Columns - product_name,product_price,Rate,Review,Summary,Sentiment

                selected_columns_fact = ['Rate','Sentiment']
                selected_columns_prod_dim = ['product_name','product_price']
                selected_columns_review_sum = ['Review','Summary']
                fact_reviews = df[selected_columns_fact]
                product_dim = df[selected_columns_prod_dim]
                review_summary_dim = df[selected_columns_review_sum]
                
                # Testing if the data is correctly imported from csv.
                #print(fact_reviews.head(),'\n',product_dim.head(),'\n',review_summary_dim.head())
                
                product_dim['product_id'] = range(1, len(product_dim) + 1)
                review_summary_dim['review_summary_id'] = range(1,len(review_summary_dim) + 1)
                final_sel_data_prod = ['product_id','product_name','product_price']
                final_sel_data_rev_sum = ['review_summary_id','Review','Summary']
                product_dimns = product_dim[final_sel_data_prod]
                review_summary_dimns = review_summary_dim[final_sel_data_rev_sum]

                print('\n',product_dimns,'\n\n',review_summary_dimns)
                product_dim.to_sql('dim_product',conn,if_exists='replace',index=False)
                review_summary_dim.to_sql('dim_review_summary',conn,if_exists='replace',index=False)
                print('Values or Records inserted into database successfully. ')

                #print(pd.read_sql('select product_id from dim_product',conn))
                df_products = pd.read_sql('select product_id from dim_product',conn)
                print('Read the table dim_product from database.')
                df_reviews_sum = pd.read_sql('select review_summary_id from dim_review_summary',conn)
                print('Read the table dim_review_summary from the database.')
                fact_mid_reviews = pd.concat([df_products,df_reviews_sum,fact_reviews],axis=1)
                fact_mid_reviews['review_id'] = range(1,len(fact_mid_reviews) +  1)
                print('\n Fact Table : \n',fact_mid_reviews.head(10))
                #fact_mid_reviews.to_sql('fact_reviews',con=conn,if_exists='replace',index=False)
            else:
                print('File doesn''t present in folder.')

        except Exception as e:
            print('Database is not initialized. ',e)

        finally:
            if conn is not None:
                conn.close()
                print('Database Disconnected.')

if __name__ == '__main__':
    in_file = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Processed_Dataset.csv'
    convert = CSV_SQL(in_file)
    convert.csv_to_sql()
