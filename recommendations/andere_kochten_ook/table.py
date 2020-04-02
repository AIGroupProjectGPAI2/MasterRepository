import csv
import thebestsql as bsql

conn = bsql.get_connection("Floris09", "huwebshop")
cur = bsql.get_cursor(conn)

def most_bought():
    orders_cats = {}
    orders = {}
    query = '''
        SELECT o.productid, c.category, o.count 
        FROM orders AS o, categories AS c, products AS p
        WHERE o.productid = p.ID
        AND p.categoriesid = c.ID
        AND o.sessionsid in (
            SELECT id 
            FROM sessions
            WHERE session_end BETWEEN (
                select session_end
                FROM sessions
                ORDER BY session_end DESC LIMIT 1) - INTERVAL '30 DAY' 
                AND 
                (select session_end
                FROM sessions
                ORDER BY session_end DESC LIMIT 1))'''
    data = bsql.select_data(cur, query)

    for row in data:
        cat = row[1]
        product_id = row[0]
        product_count = row[2]
        if cat not in orders_cats:
            orders_cats[cat] = {}
        if product_id in orders_cats[cat]:
            orders_cats[cat][product_id] += product_count
        else:
            orders_cats[cat][product_id] = product_count

        if product_id in orders:
            orders[product_id] += product_count
        else:
            orders[product_id] = product_count
    cats_popular = {}
    for k in orders_cats.keys():
        orders_cats[k] = sorted(orders_cats[k].items(), key=lambda x: x[1], reverse=True)
    orders = sorted(orders.items(), key=lambda x: x[1], reverse=True)
    return orders, orders_cats
    # for row in data:
    #     if row[0] in orders:
    #         orders[row[0]] += row[1]
    #     else:
    #         orders[row[0]] = row[1]
    # sorted_orders = {k: v for k, v in sorted(orders.items(), key=lambda item: item[1])}
    # return sorted_orders

def make_full_lists():
    most_bought_products, most_bought_products_cat = most_bought()
    simple_dict = {}
    for i in most_bought_products_cat:
        simple_dict[i] = []
        for j in most_bought_products_cat[i]:
            if len(simple_dict[i]) < 5:
                simple_dict[i].append(j[0])
            else:
                break
        if len(simple_dict[i]) < 5:
            need = 5 - len(simple_dict[i])
            need_prods = most_bought_products[0:need]
            for j in need_prods:
                simple_dict[i].append(j[0])
    return simple_dict

def upload_to_csv(filename):
    most_bought_products = make_full_lists()
    orders = []
    with open(filename + '.csv', 'w', newline='', encoding='utf-8') as mpp:
        mpp_fieldnames = ["category", "productid1", "productid2", "productid3", "productid4", "productid5"]
        mpp_writer = csv.DictWriter(mpp, fieldnames=mpp_fieldnames)
        mpp_writer.writeheader()

        for i in most_bought_products:
            if i is not None:
                p1 = most_bought_products[i][0]
                p2 = most_bought_products[i][1]
                p3 = most_bought_products[i][2]
                p4 = most_bought_products[i][3]
                p5 = most_bought_products[i][4]
                mpp_writer.writerow(
                    {
                        "category": i,
                        "productid1": p1,
                        "productid2": p2,
                        "productid3": p3,
                        "productid4": p4,
                        "productid5": p5
                    }
                )

        # for i in most_bought_products.keys():
        #     print(i, most_bought_products[i])
        #     orders.append(i)
        # orders.reverse()
        # for i in orders:
        #     mpp_writer.writerow(
        #         {
        #             "productid": i
        #         }
        #     )
    return filename

def create_table(tablename):
    bsql.do_query(conn, cur, f'''
        DROP TABLE IF EXISTS {tablename} CASCADE;
    ''')
    bsql.do_query(conn, cur, f'''
        CREATE TABLE {tablename} (category varchar(255) NOT NULL, productsID1 varchar(255) NOT NULL, productsID2 varchar(255) NOT NULL, productsID3 varchar(255) NOT NULL, productsID4 varchar(255) NOT NULL, productsID5 varchar(255) NOT NULL);
    ''')
    bsql.do_query(conn, cur, f'''
        ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305528 FOREIGN KEY (productsID1) REFERENCES products (ID);
        ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305521 FOREIGN KEY (productsID2) REFERENCES products (ID);
        ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305522 FOREIGN KEY (productsID3) REFERENCES products (ID);
        ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305523 FOREIGN KEY (productsID4) REFERENCES products (ID);
        ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305524 FOREIGN KEY (productsID5) REFERENCES products (ID);
    ''')

def upload_to_table(tablename):
    filename = upload_to_csv(tablename)
    bsql.copy_file_to_table(conn, cur, filename)

create_table('andere_kochten_ook')
upload_to_table('andere_kochten_ook')