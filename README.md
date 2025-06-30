# Top10-Banks


Code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.

It is an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

Logs the progress of the code at different stages in a file code_log.txt. 

Extracts the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.

Transforms the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.

Loads the transformed dataframe to an output CSV file. 

Load the transformed dataframe to an SQL database server as a table.

Runs queries on the database table.

