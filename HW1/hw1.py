import random
import sqlite3
import dask.dataframe as dd
import pandas as pd
import pyarrow.csv as pc
import pyarrow.parquet as pq
import os
import csv

'''
Assignment #1

This assignment was written as a script, beginning on line 206.
'''

''' TASK 1: CSV and SQL '''


# create local CSV file with columns id, fruit, price, color
def create_csv(file_name):

    column_names = ['id', 'fruit', 'price', 'color']
    fruit_values = ['Orange', 'Grape', 'Apple', 'Banana', 'Pineapple', 'Avocado']
    color_values = ['Red', 'Green', 'Yellow', 'Blue']

    with open(file_name, 'w') as file:
        writer = csv.writer(file)
        #writer.writerow(column_names)

        for row in range(0, 1000000):
            id_value = row + 1
            price_value = random.randint(10, 100)
            row = [id_value, random.choice(fruit_values), price_value, random.choice(color_values)]
            writer.writerow(row)

    print('CSV created..')


# create mydb.db sqlite3 database and create mydata table within it
def create_table():
    connection = sqlite3.connect('mydb.db')
    cursor = connection.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS mydata
                  (id INTEGER, fruit TEXT, price INTEGER, color TEXT)''')

    connection.commit()
    print('Database and table created..')
    connection.close()


# load 'mydata.csv' into 'mydata' table
def load_data(file_name):
    conn = sqlite3.connect('mydb.db')
    cursor = conn.cursor()

    with open(file_name, 'r') as file:

        csv_reader = csv.reader(file)
        header = next(csv_reader)
        print('Loading data into SQLITE table...')

        for row in csv_reader:
            cursor.execute("INSERT INTO mydata  (id, fruit, price, color) VALUES (?, ?, ?, ?)",
                      (row[0], row[1], row[2], row[3]))

            if row == 500000:
                print('Halfway there...')

    print('Data loaded..')
    conn.commit()
    conn.close()


''' TASK 2: CSV and Parquet '''


# read 'mydata.csv' file and count number of lines
def count_lines(file_name):

    with open(file_name, "r") as f:
        reader = csv.reader(f, delimiter=",")
        data = list(reader)
        line_count = len(data)
    print('Lines counted..')
    return line_count


# Use PyArrow to create Parquet file from mydata.csv
def create_parquet_pyarrow(file_name):

    table = pc.read_csv(file_name)
    pq.write_table(table, 'mydatapyarrow.parquet')
    print('Parquet file from PyArrow has been created...')


# Use Dask to create Parquet file from mydata.csv
def create_parquet_dask(file_name):

    dask_df = dd.read_csv(file_name)
    dask_df.to_parquet('mydatadask.parquet')
    print('Parquet file from Dask has been created...')


# Use Pandas to create Parquet file from mydata.csv
def create_parquet_pandas(file_name):

    df = pd.read_csv(file_name)
    df.to_parquet('mydatapandas.parquet')
    print('Parquet file from Pandas has been created...')


''' TASK 3: Split CSV files '''


# calculate size of CSV in bytes
def middle(file_name):

    csv_bytes = os.path.getsize(file_name)
    middle = int(csv_bytes / 2)
    print('Middle point found at... %s bytes' % (middle))
    return middle


# counts number of rows by reading the byte range of CSV from 0 until middle
def first_chunk(file_name, middle):

    file = open(file_name, "rb")
    results = file.readlines(middle)

    print('First chunk found...')
    return results


# counts number of rows by reading byte range of cSV from middle to end of file
def last_chunk(file_name, middle):

    file= open(file_name, "rb")
    file.seek(middle+1, 0)
    results = file.readlines()

    print('Last chunk found...')
    return results


#process “mydata.csv “ in defined chunjs and count number of lines for each chunk
def lines_per_chunk(file_name, chunk_size_mb):

    file = open(file_name, "rb")
    total_csv_bytes = os.path.getsize(file_name)

    chunk_size_bytes = int(chunk_size_mb * 1000000)
    total_bytes_chunks = []

    current_byte_start = 0
    remaining_byte_range = total_csv_bytes
    for byte_chunk in range(0, total_csv_bytes, chunk_size_bytes):

        line_count = 0
        if remaining_byte_range <= chunk_size_bytes:

            file.seek(current_byte_start+1, 0)
            results = file.read(remaining_byte_range).decode(encoding='utf-8')
            for line in results:
                if line == '\n':
                    line_count += 1
        else:
            file.seek(current_byte_start+1, 0)
            results = file.read(chunk_size_bytes).decode(encoding='utf-8')
            for line in results:
                if line == '\n':
                    line_count += 1

            current_byte_start += chunk_size_bytes
            remaining_byte_range = remaining_byte_range - chunk_size_bytes

        total_bytes_chunks.append(line_count)

    return total_bytes_chunks

if __name__ == '__main__':

    csv_file_name = 'mydata.csv'

    ''' Create local CSV file'''
    create_csv(csv_file_name)
    print()

    ''' TASK 1: CSV and SQL '''
    print('Starting Task 1...')

    ''' 1. create SQLite database “mydb.db” and create a table “mydata” '''
    create_table()

    ''' 2. load “mydata.csv” into “mydata” table'''
    load_data(csv_file_name)

    ''' 3. Write 2  SQL statements, explain predicate and projection:
     - Projection is column based filtering, while predicate is row based filtering
    
     - select fruit, price from mydata where price > 50 and color=’Green’;
     Projection is in the select clause to only return two (fruit, price) of the four columns
     Predicate is in the where clause to only return rows above a certain price and a certain color
     
     - select color, count(*) as number from mydata where fruit='Grape' group by color;
     Projection is selecting the color and using an aggregate function to count the relevant rows
     Predicate is selecting only the rows with a certain fruit type
    '''
    print()

    ''' TASK 2: CSV and Parquet '''
    print('Starting Task 2...')

    ''' 1. Read “mydata.csv” file and count number of lines '''
    print('Total number of row: '+ str(count_lines(csv_file_name))) # 1,000,000 (not including the header)

    ''' 2. Create parquet file using PyArrow '''
    create_parquet_pyarrow(csv_file_name)

    ''' 3. Create Parquet file using Dask '''
    create_parquet_dask(csv_file_name)

    '''4. Create Parquet files using Pandas '''
    create_parquet_pandas(csv_file_name)

    '''5. Examine generated Parquet files: why was Dask generated differently?
     - Dask creates the file in parallel (since it is written with parallel computing in mind) 
      and it writes each partition to a separate parquet file. 
     - Pandas actually leverages the PyArrow library to write Parquet files, 
      so both Pandas and PyArrow Parquet files are generated in the same way.
     - Pandas, PyArrow and Dask all create a Parquet file from a DataFrame object. 
      The difference is that the Dask DF implemented a blocked parallel DF object, 
      where one dask DF is comprised of many Pandas DFs that have been separated along the index. 
      Therefore, one task on a Dask DF triggers many parallel Pandas operations to model parallelism.
    '''
    print()

    ''' TASK 3: Split CSV files '''
    print('Starting Task 3...')

    '''1. Calculates size of “mydata.csv” in bytes and defines middle '''
    middle = middle(csv_file_name)

    '''2a. Count # of rows by reading the byte range of file, from 0 till the “middle”.'''
    first_chunk_result = first_chunk(csv_file_name, middle)
    first_chunk_length = len(first_chunk_result)

    '''2b. Count # of rows by reading byte range of  file from the “middle”+1 till the end '''
    last_chunk_result = last_chunk(csv_file_name, middle)
    last_chunk_length = len(last_chunk_result)

    print('Total number of lines, counted by bytes: ' + str(first_chunk_length+last_chunk_length))

    '''3. Why is total number of lines from the first chunk and second chunk is larger? 
    The total number of lines from the first and second chunk is 1,000,001, which is larger 
    than the number of lines calculated previously by 1. This is happening because the data 
    is being split by bytes, which probably is not splitting the rows exactly at a line break. 
    Therefore, there is a row that is counted twice.
    '''

    '''4. Suggest an algorithm to resolve the issue from the step 3
     We know from testing that this line overlap is happening with the last line of the first 
     chunk and the first line of the second chunk. To resolve this issue, we suggest an algorithm
     that loops over the total bytes of the file by the given chunk size and for that chunk size, 
     count the lines when every '\n' character appears.'''
    middle_mb = middle/1000000
    total_bytes = lines_per_chunk(file_name='mydata.csv', chunk_size_mb=middle_mb)
    print('Total number of bytes calculated per defined chunk (%s MB)... %s' % (str(middle_mb), sum(x for x in total_bytes)))

    '''5. Write a function that process “mydata.csv “ in defined chunjs and count number of lines 
    for each chunk. Define chunk size to be 16MB.  '''
    total_bytes = lines_per_chunk(file_name='mydata.csv', chunk_size_mb=16)
    print('Total number of bytes calculated per defined chunk (%s MB)... %s' % (str(16), sum(x for x in total_bytes)))
