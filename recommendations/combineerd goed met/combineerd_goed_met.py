import csv, json
import thebestsql as bsql

conn = bsql.get_connection("Floris09", "huwebshop")
cur = bsql.get_cursor(conn)


def get_all_order():
    data = bsql.select_data(cur, "SELECT * FROM orders;")
    print("Got all the orders")
    return data


def link_order():
    all_orders = get_all_order()
    linked_orders = {}
    keys_to_remove = []
    for row in all_orders:
        if row[0] not in linked_orders:
            linked_orders[row[0]] = []
        linked_orders[row[0]].append(row[1])
    for linked_order in linked_orders:
        if len(linked_orders[linked_order]) < 2:
            keys_to_remove.append(linked_order)
    for key in keys_to_remove:
        del linked_orders[key]
    print("Linked all the orders")
    return linked_orders


def get_combis():
    linked_orders = link_order()
    combis = {}
    for key in linked_orders:
        for pid in linked_orders[key]:
            if pid not in combis:
                combis[pid] = {}
    print("Got all the products")
    c = 0
    for key in combis:
        for sesskey in linked_orders:
            if key in linked_orders[sesskey]:
                for p in linked_orders[sesskey]:
                    # print(linked_orders[sesskey])
                    if p != key:
                        if p in combis[key]:
                            combis[key][p] += 1
                        else:
                            combis[key][p] = 1
        c += 1
        if c % 100 == 0:
            print(c)

    # with open('test.json', 'w') as tf:
    #     tf.write(json.dumps(combis, indent=4))
    return combis


def sim_products():
    combis = get_combis()
    for combi in combis:
        # print(combi)
        simproducts_data = bsql.select_data(cur, f"SELECT * FROM soort_gelijke_producten WHERE productid = '{combi}';")
        # print(f"SELECT * FROM soort_gelijke_producten WHERE productid = '{combi}';")
        for row in simproducts_data:
            # print(combi, "lijkt op", row)
            for i in range(1, 6):
                if row[i] in combis:
                    # print(row[i], "WEL in", combis)
                    # print(combis[row[i]])
                    for p in combis[row[i]]:
                        if p in combis[combi]:
                            combis[combi][p] += 1
                        else:
                            combis[combi][p] = 1
                else:
                    pass
                    # print(row[i], "not in", combis)

    return combis
    with open('test.json', 'w') as tf:
        tf.write(json.dumps(combis, indent=4))


def order_dict_in_dict():
    unsorted_combis = sim_products()
    sorted_combis = {}
    for i in unsorted_combis:
        # tmp_sorted = sorted(unsorted_combis[i].items(), key=lambda x: x[1], reverse=True)
        sorted_combis[i] = sorted(unsorted_combis[i].items(), key=lambda x: x[1], reverse=True)
    # print(sorted_combis)
    return sorted_combis
    print(sorted_combis["26085"][1][0])
    with open('test.json', 'w') as tf:
        tf.write(json.dumps(sorted_combis, indent=4))

def write_to_csv():
    sorted_combis = order_dict_in_dict()
    with open('combineert_goed_met.csv', 'w', newline='', encoding='utf-8') as cgm_file:
        cgm_fieldnames = ["productid", "productid1", "productid2", "productid3", "productid4"]
        cgm_writer = csv.DictWriter(cgm_file, fieldnames=cgm_fieldnames)
        cgm_writer.writeheader()
        for i in sorted_combis:
            key = i
            c = get_category(key)
            sc = get_subcategory(key)
            sims = get_single_sim(key)
            prio1 = []
            prio2 = []
            prio3 = []
            for j in range(0, len(sorted_combis[i])):
                if sorted_combis[i][j][1] > 4:
                    #print(sorted_combis[i][j])
                    if get_category(sorted_combis[i][j][0]) == c:
                        if get_subcategory(sorted_combis[i][j][0]) == sc:
                            prio1.append(sorted_combis[i][j][0])
                        else:
                            prio2.append(sorted_combis[i][j][0])
                    else:
                        prio3.append(sorted_combis[i][j][0])
            if len(prio1) < 4:
                need = 4 - len(prio1)
                if len(prio2) >= need:
                    prio1 = prio1 + prio2[:need]
                else:
                    prio1 = prio1 + sims[:need]
            cgm_writer.writerow(
                {
                    "productid": key,
                    "productid1": prio1[0],
                    "productid2": prio1[1],
                    "productid3": prio1[2],
                    "productid4": prio1[3]
                }
            )




def get_category(product_id):
    query = f"""SELECT category FROM categories
                WHERE id =(
                    SELECT categoriesid FROM products
                    WHERE id = '{product_id}')"""
    data = bsql.select_data(cur, query)
    c = None
    for row in data:
        c = row[0]
    return c


def get_subcategory(product_id):
    query = f"""SELECT subcategory FROM categories
                WHERE id =(
                    SELECT categoriesid FROM products
                    WHERE id = '{product_id}')"""
    data = bsql.select_data(cur, query)
    sc = None
    for row in data:
        sc = row[0]
    return sc


def get_single_sim(product_id):
    query = f"""SELECT * FROM soort_gelijke_producten
                WHERE productid = '{product_id}';"""
    data = bsql.select_data(cur, query)
    sim = []
    for row in data:
        for i in range(1, 6):
            sim.append(row[i])
    return sim

def write_to_db():
    bsql.copy_file_to_table(conn, cur, 'combineert_goed_met')

tablename = 'combineert_goed_met'

bsql.do_query(conn, cur, f'''
            DROP TABLE IF EXISTS {tablename} CASCADE;
        ''')
bsql.do_query(conn, cur, f'''
                CREATE TABLE {tablename} (productID varchar(255) NOT NULL, productID1 varchar(255) NOT NULL, productID2 varchar(255) NOT NULL, productID3 varchar(255) NOT NULL, productID4 varchar(255) NOT NULL);
            ''')
bsql.do_query(conn, cur, f'''
    ALTER TABLE {tablename} ADD CONSTRAINT FKmost_poqul305529 FOREIGN KEY (productID) REFERENCES products (ID);
    ALTER TABLE {tablename} ADD CONSTRAINT FKmost_powul305533 FOREIGN KEY (productID1) REFERENCES products (ID);
    ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popurl305534 FOREIGN KEY (productID2) REFERENCES products (ID);
    ALTER TABLE {tablename} ADD CONSTRAINT FKmost_poprul305535 FOREIGN KEY (productID3) REFERENCES products (ID);
    ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popual305536 FOREIGN KEY (productID4) REFERENCES products (ID);
''')

write_to_csv()
write_to_db()
