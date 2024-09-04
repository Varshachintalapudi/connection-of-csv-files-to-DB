import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker

# Define the MySQL connection parameters
username = 'root'
password = 'Varsha@2003'
host = 'localhost'
port = 3306

csv_file_path1 = 'C:/ProgramData/Microsoft/Windows/Start Menu/Programs/MySQL/MySQL Server 8.0/Uploads/student.csv'
csv_file_path2 = 'C:/ProgramData/Microsoft/Windows/Start Menu/Programs/MySQL/MySQL Server 8.0/Uploads/department.csv'

# Create a function to generate the database name dynamically
def generate_database_name(csv_file_path1, csv_file_path2):
    db_name = f"{csv_file_path1.split('/')[-1].split('.')[0]}_{csv_file_path2.split('/')[-1].split('.')[0]}"
    return db_name

# Generate the database name
db_name = generate_database_name(csv_file_path1, csv_file_path2)

# Create a SQLAlchemy engine
engine = create_engine(f'mysql+mysqldb://root:Varsha%402003@localhost:3306/{db_name}')

# Create a declarative base class
Base = declarative_base()

# Define a function to create a table class dynamically
def create_table_class(csv_file_path):
    table_name = csv_file_path.split('/')[-1].split('.')[0]
    df = pd.read_csv(csv_file_path)
    
    # Ensure 'id' column exists and is integer type
    if 'id' not in df.columns:
        df['id'] = range(1, len(df) + 1)
    df['id'] = df['id'].astype(int)
    
    columns = {
        '__tablename__': table_name,
        '__table_args__': {'extend_existing': True},
        'id': Column(Integer, primary_key=True)
    }
    for column_name, column_type in df.dtypes.items():
        if column_name != 'id':
            if column_type == 'int64':
                columns[column_name] = Column(Integer)
            elif column_type == 'float64':
                columns[column_name] = Column(Float)
            else:
                columns[column_name] = Column(String(255))
    
    table_class = type(table_name, (Base,), columns)
    return table_class

# Create table classes for each CSV file
TableClass1 = create_table_class(csv_file_path1)
TableClass2 = create_table_class(csv_file_path2)

# Create a session maker
Session = sessionmaker(bind=engine)

# Define a function to create the database and tables
def create_database_and_tables():
    # Create the database and tables
    Base.metadata.create_all(engine)

    # Synchronize the database with the CSV files
    update_tables()

# Define a function to update the tables when the CSV files change
from sqlalchemy import text

# Define a function to update the tables when the CSV files change
def update_tables():
    # Read the updated CSV files
    df1 = pd.read_csv(csv_file_path1)
    df2 = pd.read_csv(csv_file_path2)

    # Truncate the tables before inserting new data
    session = Session()
    session.execute(text(f"TRUNCATE TABLE {TableClass1.__tablename__}"))
    session.execute(text(f"TRUNCATE TABLE {TableClass2.__tablename__}"))
    session.commit()
    
    # Insert the data into the tables
    for index, row in df1.iterrows():
        table_data1 = TableClass1(**row.to_dict())
        session.add(table_data1)
    for index, row in df2.iterrows():
        table_data2 = TableClass2(**row.to_dict())
        session.add(table_data2)

    session.commit()

# Ensure the database and tables are created on script execution
create_database_and_tables()
