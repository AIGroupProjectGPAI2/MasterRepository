import psycopg2

c = psycopg2.connect("dbname=postgres user=postgres password=niels16") #TODO: edit this.
cur = c.cursor()


cur.execute("DROP TABLE IF EXISTS products CASCADE")
cur.execute("DROP TABLE IF EXISTS categories CASCADE")
cur.execute("DROP TABLE IF EXISTS profile_previously_viewed CASCADE")
cur.execute("DROP TABLE IF EXISTS profiles CASCADE")
cur.execute("DROP TABLE IF EXISTS sessions CASCADE")
cur.execute("DROP TABLE IF EXISTS orders CASCADE")
cur.execute("DROP TABLE IF EXISTS doelgroepen CASCADE")
cur.execute("DROP TABLE IF EXISTS brands CASCADE")

cur.execute("""CREATE TABLE products (
                    ID varchar(255) NOT NULL,
                    doelgroepenID varchar(255) NOT NULL,
                    categoriesID varchar(255) NOT NULL, 
                    brandID varchar(255) NOT NULL,
                    name varchar(255), 
                    description varchar(900), 
                    herhaalaankopen bool, 
                    price int4,
                    inhoud varchar(255), 
                    discount int4, 
                    PRIMARY KEY (ID)); """)

cur.execute("""CREATE TABLE categories (
                    ID varchar(255) NOT NULL, 
                    category varchar(255), 
                    subcategory varchar(255), 
                    subsubcategory varchar(255), 
                    PRIMARY KEY (ID)); """)

cur.execute("""CREATE TABLE profile_previously_viewed (
                    profileID varchar(255) NOT NULL, 
                    productID varchar(255) NOT NULL); """)

cur.execute("""CREATE TABLE profiles (
                    ID varchar(255) NOT NULL, 
                    segment varchar(255), 
                    PRIMARY KEY (ID)); """)

cur.execute("""CREATE TABLE sessions (
                    ID varchar(255) NOT NULL, 
                    profilesID varchar(255) NOT NULL, 
                    device_type varchar(255), 
                    session_start timestamp, 
                    session_end timestamp
                    PRIMARY KEY (ID)); """)

cur.execute("""CREATE TABLE orders (
                    sessionsID varchar(255) NOT NULL, 
                    productID varchar(255) NOT NULL, 
                    PRIMARY KEY (sessionsID, productID)); """)

cur.execute("""CREATE TABLE doelgroepen (
                    ID varchar(255) NOT NULL, 
                    target_audience varchar(255), 
                    PRIMARY KEY (ID)); """)

cur.execute("""CREATE TABLE brands (
                    ID varchar(255) NOT NULL, 
                    brand varchar(255), 
                    PRIMARY KEY (ID)); """)


cur.execute("""ALTER TABLE sessions ADD CONSTRAINT FKsessions800212 FOREIGN KEY (profilesID) REFERENCES profiles (ID); """)
cur.execute("""ALTER TABLE orders ADD CONSTRAINT FKorder8292 FOREIGN KEY (sessionsID) REFERENCES sessions (ID); """)
cur.execute("""ALTER TABLE orders ADD CONSTRAINT FKorder585286 FOREIGN KEY (productID) REFERENCES products (ID); """)
cur.execute("""ALTER TABLE products ADD CONSTRAINT FKproducts292977 FOREIGN KEY (doelgroepenID) REFERENCES doelgroepen (ID); """)
cur.execute("""ALTER TABLE products ADD CONSTRAINT FKproducts732050 FOREIGN KEY (categoriesID) REFERENCES categories (ID); """)
cur.execute("""ALTER TABLE products ADD CONSTRAINT FKproducts907628 FOREIGN KEY (brandID) REFERENCES brands (ID); """)
cur.execute("""ALTER TABLE profile_previously_viewed ADD CONSTRAINT FKprofile_pr728803 FOREIGN KEY (productID) REFERENCES products (ID); """)
cur.execute("""ALTER TABLE profile_previously_viewed ADD CONSTRAINT FKprofile_pr801267 FOREIGN KEY (profileID) REFERENCES profiles (ID); """)


c.commit()
cur.close()
c.close()