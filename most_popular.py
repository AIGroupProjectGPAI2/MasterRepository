import psycopg2, csv

c = psycopg2.connect("dbname=huwebshop user=postgres password=Floris09")
cur = c.cursor()



def get_data():
    cur.execute('''
    SELECT productid, count FROM orders WHERE sessionsid in (
    	SELECT id FROM sessions
    	WHERE session_end BETWEEN (
    	select session_end
    	FROM sessions
    	ORDER BY session_end DESC LIMIT 1) - INTERVAL '30 DAY' AND (select session_end
    	FROM sessions
    	ORDER BY session_end DESC LIMIT 1)
    )
    ''')
    fetched_data = cur.fetchall()
    return fetched_data

def most_bought():
    orders = {}
    data = get_data()
    for row in data:
        if row[0] in orders:
            orders[row[0]] += row[1]
        else:
            orders[row[0]] = row[1]
    sorted_orders = {k: v for k, v in sorted(orders.items(), key=lambda item: item[1])}
    return sorted_orders

def upload_to_csv(filename):
    most_bought_products = most_bought()
    orders = []
    with open(filename + '.csv', 'w', newline='', encoding='utf-8') as mpp:
        mpp_fieldnames = ["productid"]
        mpp_writer = csv.DictWriter(mpp, fieldnames=mpp_fieldnames)
        mpp_writer.writeheader()
        for i in most_bought_products.keys():
            print(i, most_bought_products[i])
            orders.append(i)
        orders.reverse()
        for i in orders:
            mpp_writer.writerow(
                {
                    "productid": i
                }
            )
    return filename

def create_table(tablename):
    cur.execute(f'''
        DROP TABLE IF EXISTS {tablename} CASCADE;
    ''')
    cur.execute(f'''
        CREATE TABLE {tablename} (productsID varchar(255) NOT NULL);
    ''')
    cur.execute(f'''
        ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305528 FOREIGN KEY (productsID) REFERENCES products (ID);
    ''')
    c.commit()

def upload_to_table(tablename):
    filename = upload_to_csv(tablename)
    with open(filename + '.csv') as csvfile:
        print("copying {}....".format(filename))
        cur.copy_expert("COPY " + filename + " FROM STDIN DELIMITER ',' CSV HEADER", csvfile)
        c.commit()

create_table('most_popular_products')
upload_to_table('most_popular_products')