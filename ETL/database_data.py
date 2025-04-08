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
            print('\n Database connected.')
            # Checks if the file is present in current working directory / folder.
            if os.path.exists(self.input_file):
                
                print('File is present in the folder : ',self.input_file)
                # print('\n Converting the processed dataset of csv format from target location / '
                # 'destination to the database for Analysis and Reporting\n')
                
                # Read the data from csv file format.
                df = pd.read_csv(self.input_file)
                # Columns - product_name,product_price,rate,review,summary,sentiment

                # '''Needed to re-structure the data or dataframe accordingly to design and store data using dimensional modeling.''' 
                selected_columns_fact = ['product_name','review','rate','sentiment']
                selected_columns_prod_dim = ['product_name','product_price']
                selected_columns_review_sum = ['product_name','review','summary']
                fact_reviews = df[selected_columns_fact]
                product_dim = df[selected_columns_prod_dim]
                review_summary_dim = df[selected_columns_review_sum]

                # Deleting the duplicates inside the dataframes of both dimensions.
                product_dim = product_dim.drop_duplicates(subset=['product_name'])
                review_summary_dim = review_summary_dim.drop_duplicates(subset=['product_name','review'])

                # Creating Id's for the product dimension table.
                product_dim['product_id'] = range(1, len(product_dim) + 1)

                # Creating Id's for the review summary dimension table.
                review_summary_dim['review_summary_id'] = range(1,len(review_summary_dim) + 1)

                # pushing data to database using .to_sql() function.
                product_dim.to_sql('dim_product',conn,if_exists='replace',index=False)
                review_summary_dim.to_sql('dim_review_summary',conn,if_exists='replace',index=False)
                print('Values or Records inserted into database - dimension tables successfully.')

                # Reading the data from database to implement or store data into the fact - table.
                df_products = pd.read_sql('select product_id,product_name from dim_product',conn)
                df_reviews_sum = pd.read_sql('select review_summary_id,product_name, review from dim_review_summary',conn)
                
                # Created a data frame that joins with two dimensional tables from database. For storing data inside fact table.
                fact_mid_reviews = fact_reviews.merge(df_products, on="product_name", how="left")\
                    .merge(df_reviews_sum, on=["product_name",'review'], how="left")

                # Created Id's for fact table.
                fact_mid_reviews['fact_id'] = range(1,len(fact_mid_reviews) +  1)

                # Then finally re-structured the data of what to store inside database.
                selected_columns_fact_t = ['fact_id','product_id','review_summary_id','rate','sentiment']
                fact_mid_reviews = fact_mid_reviews[selected_columns_fact_t]
               
               # Last step is to push data to database.
                fact_mid_reviews.to_sql('fact_reviews_product_summary',conn,if_exists='replace',index=False,chunksize=4000)
                # print('\n',fact_mid_reviews.head(20))
                print('Successfully Loaded Fact Table records into database.')

            else:
                print('File doesn''t present in folder.')

        except Exception as e:
            # Displays message if there is problem with any data working from database - postgreSQL.
            print('Database is not initialized. ',e)

        finally:
            # Finally if working with database is finished then the connection gets commited means data will store permanent to database.  
            # And closes the Database connection.
            if conn is not None:
                conn.commit()
                conn.close()
                print('Database Disconnected.')

if __name__ == '__main__':
    processed_file = r'D:\NikhilData\Desktop\Webpages\Python Practice\Customer Review Insights Project\Project\data\Processed_Dataset.csv'
    db_loader = CSV_SQL(processed_file)
    db_loader.csv_to_sql()
    
    