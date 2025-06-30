#Project Scenario:
'''You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.

Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'''

# Importing the required libraries
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = r'C:\Users\user1\Desktop\IBM\Course_3_Projects\Final_project\largest_banks_data.csv'
db_path = r'C:\Users\user1\Desktop\IBM\Course_3_Projects\Final_project\Banks.db'
log_file = r'C:\Users\user1\Desktop\IBM\Course_3_Projects\Final_project\code_log.txt'
table_name = 'Larget_banks'
table_attribs = ['Name','MC_USD_Billion']
ex_rate_csv = r'C:\Users\user1\Desktop\IBM\Course_3_Projects\Final_project\exchange_rate.csv'
#Table Attributes (final)=	Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion


'''Task 1:
Write a function log_progress() to log the progress of the code at different stages in a file code_log.txt. Use the list of log points provided to create log entries as every stage of the code.

Task 2:
Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.
a. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
b. Write the code for a function extract() to perform the required data extraction.
c. Execute a function call to extract() to verify the output.

Task 3:
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
a. Write the code for a function transform() to perform the said task.
b. Execute a function call to transform() and verify the output.

Task 4:
Load the transformed dataframe to an output CSV file. Write a function load_to_csv(), execute a function call and verify the output.

Task 5:
Load the transformed dataframe to an SQL database server as a table. Write a function load_to_db(), execute a function call and verify the output.

Task 6:
Run queries on the database table. Write a function load_to_db(), execute a given set of queries and verify the output.

Task 7:
Verify that the log entries have been completed at all stages by checking the contents of the file code_log.txt'''

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

# Code for ETL operations on Country-GDP data

def extract(url, table_attribs): #This function extracts the required information from the website and saves it to a dataframe. The function returns the dataframe for further processing.
    
    # Extract the web page as text. AND Parse the text into an HTML object.
    html_page = requests.get(url).text
    soup = bs(html_page, 'html.parser')
    # Create an empty pandas DataFrame named df with columns as the table_attribs.    
    extracted_data = pd.DataFrame(columns=table_attribs)
       
        # Extract all 'tbody' attributes of the HTML object and then extract all the rows of the index 0 table using the 'tr' attribute.
    tables = soup.find_all('tbody')
    rows = tables[0].find_all('tr')
    
     # Check the contents of each row, having attribute ‘td’
     # a. The row should not be empty.
     # b. The second column should contain a hyperlink.
    for row in rows:
        col = row.find_all('td')
        bank = row.find_all('a')
        
        if len(col) !=0 and len(bank) != 0:
            data_dict = {'Name': str(bank[1].contents[0].text),
                         "MC_USD_Billion": float(str(col[2].contents[0].text))}
            df1 = pd.DataFrame(data_dict, index=[0])
            extracted_data = pd.concat([extracted_data,df1], ignore_index=True)    
    log_progress('Data exctraction complete. Initiating Transformation process')       
    return(extracted_data)


def transform(df, ex_rate_csv):
    ''' This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, each containing the transformed version of Market Cap column to respective currencies'''
    
    exchange_rate_dict = pd.read_csv(ex_rate_csv).set_index('Currency').to_dict()['Rate']
    #df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df = df.assign(MC_EUR_Billion=round(df['MC_USD_Billion']*exchange_rate_dict['EUR'],2))
    df = df.assign(MC_GBP_Billion=round(df['MC_USD_Billion']*exchange_rate_dict['GBP'],2))
    df = df.assign(MC_INR_Billion=round(df['MC_USD_Billion']*exchange_rate_dict['INR'],2))

    log_progress('Data transformation complete. Initiating load process')
    return(df)
    
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)
    log_progress('Data Saved to CSV file')
    
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Excluding queries")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    log_progress('Process Complete')
    print(query_output)



''' Here, you define the required entities and call the relevant functions in the correct order to complete the project. Note that this portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process')

extracted_data = extract(url, table_attribs)

transformed_data=transform(extracted_data,ex_rate_csv)
'''print(transformed_data['MC_EUR_Billion'][4]) #transformed data == df
print(transformed_data)'''

load_to_csv(transformed_data,csv_path)

conn = sqlite3.connect(db_path)
log_progress('SQL connection initiated')

load_to_db(transformed_data,conn,table_name)
query1 = f'SELECT * FROM {table_name}'
query2 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
query3 = f'SELECT Name FROM {table_name} LIMIT 5'
print(run_query(query1,conn))
print(run_query(query2,conn))
print(run_query(query3,conn))

conn.close()
log_progress('Server connection closed')