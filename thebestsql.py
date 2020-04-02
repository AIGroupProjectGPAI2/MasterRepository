import psycopg2


def get_connection(password, database, user="postgres", port="5432", host="localhost"):
    connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)
    return connection


def get_cursor(connection):
    cursor = connection.cursor()
    return cursor


def select_data(cursor, query):
    cursor.execute(query)
    fetched_data = cursor.fetchall()
    return fetched_data


def copy_file_to_table(connection, cursor, filename):
    try:
        with open(filename + '.csv', encoding='utf-8') as csvfile:
            print("copying {}....".format(filename))
            cursor.copy_expert("COPY " + filename + " FROM STDIN DELIMITER ',' CSV HEADER", csvfile)
            connection.commit()
        response = True
    except Exception as e:
        print(e)
        response = False
    return response


def copy_files_to_tables(connection, cursor, filenames):
    for filename in filenames:
        copy_file_to_table(connection, cursor, filename)


def end_session(connection, cursor):
    cursor.close()
    connection.close()


def reset_db(connection, cursor):
    try:
        cursor.execute('''
        ALTER TABLE orders DROP CONSTRAINT FKorders185503;
        ALTER TABLE sessions DROP CONSTRAINT FKsessions800212;
        ALTER TABLE orders DROP CONSTRAINT FKorders608508;
        ALTER TABLE products DROP CONSTRAINT FKproducts292977;
        ALTER TABLE products DROP CONSTRAINT FKproducts732050;
        ALTER TABLE products DROP CONSTRAINT FKproducts907628;
        ALTER TABLE profiles_previously_viewed DROP CONSTRAINT FKprofiles_p10964;
        ALTER TABLE profiles_previously_viewed DROP CONSTRAINT FKprofiles_p938499;''')
    except:
        print("Contrains not dropped because they are not there")
    connection.commit()
    cursor.execute('''
    DROP TABLE IF EXISTS doelgroepen CASCADE;
    DROP TABLE IF EXISTS categories CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS sessions CASCADE;
    DROP TABLE IF EXISTS profiles CASCADE;
    DROP TABLE IF EXISTS products CASCADE;
    DROP TABLE IF EXISTS brands CASCADE;
    DROP TABLE IF EXISTS profiles_previously_viewed CASCADE;
    CREATE TABLE doelgroepen (ID varchar(255) NOT NULL, target_audience varchar(255), PRIMARY KEY (ID));
    CREATE TABLE categories (ID varchar(255) NOT NULL, category varchar(255), subcategory varchar(255), subsubcategory varchar(255), PRIMARY KEY (ID));
    CREATE TABLE orders (sessionsID varchar(255) NOT NULL, productID varchar(255) NOT NULL, count int4 NOT NULL, PRIMARY KEY (sessionsID, productID));
    CREATE TABLE sessions (ID varchar(255) NOT NULL, profilesID varchar(255) NOT NULL, device_type varchar(255), session_start timestamp, session_end timestamp, PRIMARY KEY (ID));
    CREATE TABLE profiles (ID varchar(255) NOT NULL, segment varchar(255), PRIMARY KEY (ID));
    CREATE TABLE products (ID varchar(255) NOT NULL, doelgroepenID varchar(255) NOT NULL, categoriesID varchar(255) NOT NULL, brandID varchar(255) NOT NULL, name varchar(255), description varchar(9000), herhaalaankopen bool, price int4, inhoud varchar(255), discount int4, PRIMARY KEY (ID));
    CREATE TABLE brands (ID varchar(255) NOT NULL, brand varchar(255), PRIMARY KEY (ID));
    CREATE TABLE profiles_previously_viewed (profileID varchar(255) NOT NULL, productID varchar(255) NOT NULL, count int4 NOT NULL);
    ALTER TABLE orders ADD CONSTRAINT FKorders185503 FOREIGN KEY (sessionsID) REFERENCES sessions (ID);
    ALTER TABLE sessions ADD CONSTRAINT FKsessions800212 FOREIGN KEY (profilesID) REFERENCES profiles (ID);
    ALTER TABLE orders ADD CONSTRAINT FKorders608508 FOREIGN KEY (productID) REFERENCES products (ID);
    ALTER TABLE products ADD CONSTRAINT FKproducts292977 FOREIGN KEY (doelgroepenID) REFERENCES doelgroepen (ID);
    ALTER TABLE products ADD CONSTRAINT FKproducts732050 FOREIGN KEY (categoriesID) REFERENCES categories (ID);
    ALTER TABLE products ADD CONSTRAINT FKproducts907628 FOREIGN KEY (brandID) REFERENCES brands (ID);
    ALTER TABLE profiles_previously_viewed ADD CONSTRAINT FKprofiles_p10964 FOREIGN KEY (profileID) REFERENCES profiles (ID);
    ALTER TABLE profiles_previously_viewed ADD CONSTRAINT FKprofiles_p938499 FOREIGN KEY (productID) REFERENCES products (ID);''')
    connection.commit()

def SQL_fetch_data(SQL):
    connection = psycopg2.connect(user="postgres",
                                  password="niels16",
                                  host="localhost",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()
    cursor.execute(SQL)
    fetched_data = cursor.fetchall()
    cursor.close()
    connection.close()
    return fetched_data

def do_query(connection, cursor, query):
    cursor.execute(query)
    connection.commit()
