import os
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to PostgreSQL database
def connect():
    # Construct connection string
    connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    
    # Create engine with autocommit set to True
    engine = create_engine(connection_string, execution_options={'autocommit': True})
    return engine

if __name__ == "__main__":
    try:
        # Connect to the database
        engine = connect()

        # 1) Execute the SQL sentences to create tables
        create_tables_sql = """
        CREATE TABLE IF NOT EXISTS publishers (
            publisher_id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        );

        CREATE TABLE IF NOT EXISTS authors (
            author_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            middle_name VARCHAR(50),
            last_name VARCHAR(100)
        );

        CREATE TABLE IF NOT EXISTS books (
            book_id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            total_pages INT,
            rating DECIMAL(4, 2),
            isbn VARCHAR(13),
            published_date DATE,
            publisher_id INT REFERENCES publishers(publisher_id)
        );

        CREATE TABLE IF NOT EXISTS book_authors (
            book_id INT REFERENCES books(book_id),
            author_id INT REFERENCES authors(author_id),
            PRIMARY KEY(book_id, author_id)
        );
        """
        engine.execute(create_tables_sql)
        print("Tables created successfully.")

        # 2) Execute the SQL sentences to insert data
        insert_data_sql = """
        INSERT INTO publishers (name) VALUES 
            ('O Reilly Media'),
            ('A Book Apart'),
            ('A K PETERS'),
            ('Academic Press'),
            ('Addison Wesley'),
            ('Albert&Sweigart'),
            ('Alfred A. Knopf');

        INSERT INTO authors (first_name, middle_name, last_name) VALUES 
            ('Merritt', NULL, 'Eric'),
            ('Linda', NULL, 'Mui'),
            ('Alecos', NULL, 'Papadatos'),
            ('Anthony', NULL, 'Molinaro'),
            ('David', NULL, 'Cronin'),
            ('Richard', NULL, 'Blum'),
            ('Yuval', 'Noah', 'Harari'),
            ('Paul', NULL, 'Albitz');

        INSERT INTO books (title, total_pages, rating, isbn, published_date, publisher_id) VALUES 
            ('Lean Software Development: An Agile Toolkit', 240, 4.17, '9780320000000', '2003-05-18', 5),
            ('Facing the Intelligence Explosion', 91, 3.87, NULL, '2013-02-01', 7),
            ('Scala in Action', 419, 3.74, '9781940000000', '2013-04-10', 1),
            ('Patterns of Software: Tales from the Software Community', 256, 3.84, '9780200000000', '1996-08-15', 1),
            ('Anatomy Of LISP', 446, 4.43, '9780070000000', '1978-01-01', 3),
            ('Computing machinery and intelligence', 24, 4.17, NULL, '2009-03-22', 4),
            ('XML: Visual QuickStart Guide', 269, 3.66, '9780320000000', '2009-01-01', 5),
            ('SQL Cookbook', 595, 3.95, '9780600000000', '2005-12-01', 7),
            ('The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 439, 4.29, '9781440000000', '2010-07-01', 6),
            ('Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 222, 3.54, '9780750000000', '2007-02-13', 7);

        INSERT INTO book_authors (book_id, author_id) VALUES 
            (1, 1),
            (2, 8),
            (3, 7),
            (4, 6),
            (5, 5),
            (6, 4),
            (7, 3),
            (8, 2),
            (9, 4),
            (10, 1);
        """
        engine.execute(insert_data_sql)
        print("Data inserted successfully.")

        # 3) Use Pandas to print one of the tables as a DataFrame
        df_publishers = pd.read_sql("SELECT * FROM publishers;", engine)
        print("Publishers Table:")
        print(df_publishers)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Ensure the engine is disposed to close the connection
        if 'engine' in locals() or 'engine' in globals():
            engine.dispose()
            print("Connection closed.")
