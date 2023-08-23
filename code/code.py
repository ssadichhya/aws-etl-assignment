import json
import boto3
import urllib3
import pandas as pd
import psycopg2
import os


s3 = boto3.client("s3")

# url = "https://www.anapioficeandfire.com/api/books"
# http = urllib3.PoolManager()


def lambda_handler(event, context):
 
    response = http.request('GET',url)
    
    #putting data into s3 bucket
    # s3.put_object(Body=response.data, Bucket = 'apprentice-training-data-dev-sadichhya-raw-data',Key='got_books_raw.json')
    
    #changing jason data into dataframe
    data = response.data.decode('utf-8')
    books_data = json.loads(data)
    df=pd.DataFrame(books_data)
    
    # Transformations
    
    #  Select specific columns
    selected_columns = df[['name', 'authors', 'numberOfPages', 'released']]

    # Rename columns
    df = df.rename(columns={'name': 'Book Name', 'authors': 'Authors', 'numberOfPages': 'Pages', 'released': 'Release Date'})

    # Convert release date to datetime format
    df['Release Date'] = pd.to_datetime(df['Release Date'])

    
    # Convert the 'numberOfPages' column to kilobytes
    df['pages_kb'] = df['Pages'] * 1024
    
    #uploading cleanded data to s3
    cleaned_json = df.to_csv(index=False)
    s3.put_object(Body=cleaned_json, Bucket = 'apprentice-training-data-dev-sadichhya-clean-data',Key='got_books_clean.csv')




    #getting data from s3
    response=s3.get_object(Bucket = 'apprentice-training-data-dev-sadichhya-clean-data',Key='got_books_clean.csv')
    df = pd.read_csv(response['Body'])
    
    
    # connect database
    try:
        connection = psycopg2.connect(
        host = os.environ['host'],
        database = os.environ['database'],
        user = os.environ['user'],
        password = os.environ['password'])

        print("connected to the database")
    except Exception as e:
        print("Error:", e)

    # create cursor
    cursor = connection.cursor()

    # # SQL query to create the "books" table
    create_table_query = '''
        CREATE TABLE etl_sadichhya_books_table (
            url varchar,
            "Book Name" varchar,
            isbn varchar,
            authors varchar,
            pages INTEGER,
            publisher varchar,
            country varchar,
            mediatype varchar,
            "Release Date" DATE,
            characters varchar,
            povCharacter varchar,
            pages_kb REAL
        );
    '''
    try:
        cursor.execute(create_table_query)
        print("Table 'books' created successfully.")
    except Exception as e:
        print("Error:", e)
    
    connection.commit()

    data_to_insert = [tuple(row) for row in df.values]

    insert_query = f"""
    INSERT INTO etl_sadichhya_books_table
    (url,"Book Name" ,isbn ,authors ,pages ,publisher,country,mediatype,"Release Date",characters,povCharacter,pages_kb )
    VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, data_to_insert)
    connection.commit()
   
    # SQL query to update the author value
    update_author_query = '''
        UPDATE etl_sadichhya_books_table
        SET authors = 'George R. R. Martin'
    '''
    
    try:
        cursor.execute(update_author_query)
        print("Table 'books' updated successfully.")
    except Exception as e:
        print("Error:", e)    
    connection.commit()



    # SQL query to replace empty list in povcharacter to null
    replace_empty_list_query = '''
        UPDATE etl_sadichhya_books_table
        SET povcharacter = NULL
        WHERE povcharacter= '[]';
    '''

     
    try:
        cursor.execute(replace_empty_list_query)
        print("Table 'books' updated successfully.")
    except Exception as e:
        print("Error:", e)    
    connection.commit()




    

    #Commit the changes and close the connection

    connection.close()




    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }




