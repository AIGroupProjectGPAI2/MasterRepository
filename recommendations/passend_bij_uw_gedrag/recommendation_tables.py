import thebestsql as bsql
import csv, random

conn = bsql.get_connection("Floris09", "huwebshop")
cur = bsql.get_cursor(conn)


def profile_doelgroep():
    profiles_orders_query = """SELECT pf.ID,
                        d.ID,
                        o.count
                        FROM profiles AS pf, 
                             sessions AS s, 
                             products AS pd, 
                             orders AS o,
                             doelgroepen AS d
                        WHERE pf.ID = s.profilesID
                          AND s.ID = o.sessionsID
                          AND o.productID = pd.ID
                          AND d.ID = pd.doelgroepenID
                        ORDER BY o.count DESC"""
    profiles_ppv_query = """SELECT ppv.profileid, d.ID, ppv.count
                            FROM profiles_previously_viewed as ppv, doelgroepen as d, products
                            WHERE ppv.productid = products.id
                            AND d.id = products.doelgroepenID"""

    orders_data = bsql.select_data(cur, profiles_orders_query)
    ppv_data = bsql.select_data(cur, profiles_ppv_query)
    all_data = orders_data + ppv_data
    print(len(orders_data), len(ppv_data), len(orders_data) + len(ppv_data), len(all_data))
    profiles = {}
    c = 0
    for row in all_data:
        c += 1
        profile_id = row[0]
        doelgroep = row[1]
        doelgroep_count = row[2]
        if profile_id in profiles:
            if doelgroep in profiles[profile_id]:
                profiles[profile_id][doelgroep] += doelgroep_count
            else:
                profiles[profile_id][doelgroep] = doelgroep_count
        else:
            profiles[profile_id] = {
                doelgroep: doelgroep_count
            }
        # print(profile_id)
    with open('profiles_doelgroepen.csv', 'w', newline='', encoding='utf-8') as pd_file:
        pd_fieldnames = ["profileID", "doelgroepID"]
        pd_writer = csv.DictWriter(pd_file, fieldnames=pd_fieldnames)
        pd_writer.writeheader()
        for profile in profiles:
            pd_writer.writerow(
                {
                    "profileID": profile,
                    "doelgroepID": max(profiles[profile], key=lambda key: profiles[profile][key])
                }
            )

    print(len(profiles))


def doelgroepen_products():
    # Welke producten zjn het populairste per doelgroep
    query = '''
        SELECT * FROM most_popular_products'''
    data = bsql.select_data(cur, query)
    doelgroepen_prods = {}
    print(len(data))
    for row in data:
        row_query = f"""
            SELECT doelgroepenid
            FROM products
            WHERE id = '{row[0]}'"""
        row_data = bsql.select_data(cur, row_query)
        for row_row in row_data:
            dgid = row_row[0]
            if dgid in doelgroepen_prods:
                if len(doelgroepen_prods[dgid]) < 5:
                    doelgroepen_prods[dgid].append(row[0])
            else:
                doelgroepen_prods[dgid] = list()
                doelgroepen_prods[dgid].append(row[0])
    for i in range(0, 17):
        if str(i) in doelgroepen_prods:
            if len(doelgroepen_prods[str(i)]) < 5:
                need = 5 - len(doelgroepen_prods[str(i)])
                need_list = data[0:need+10]
                need_list = random.sample(need_list, need)
                for n in need_list:
                    doelgroepen_prods[str(i)].append(n[0])
        else:
            doelgroepen_prods[str(i)] = []
            need = 5 - len(doelgroepen_prods[str(i)])
            need_list = data[0:need + 10]
            need_list = random.sample(need_list, need)
            for n in need_list:
                doelgroepen_prods[str(i)].append(n[0])
    with open('profiles_doelgroepen.csv', 'r') as pd, open('passend_bij_uw_gedrag.csv', 'w', newline='', encoding='utf-8') as pbug_file:
        pd_reader = csv.reader(pd)
        pbug_fieldnames = ["profileid", "productid1", "productid2", "productid3", "productid4", "productid5"]
        pbug_writer = csv.DictWriter(pbug_file, fieldnames=pbug_fieldnames)
        pbug_writer.writeheader()
        lc = 0
        for i in pd_reader:
            if lc != 0:
                matching_products = doelgroepen_prods[i[1]]
                pbug_writer.writerow(
                    {
                        "profileid": i[0],
                        "productid1": matching_products[0],
                        "productid2": matching_products[1],
                        "productid3": matching_products[2],
                        "productid4": matching_products[3],
                        "productid5": matching_products[4]
                    }
                )
            else:
                lc += 1

    print(doelgroepen_prods)


def create_fill_tables(tablenames):
    profile_doelgroep()
    doelgroepen_products()
    for tablename in tablenames:
        bsql.do_query(conn, cur, f'''
            DROP TABLE IF EXISTS {tablename} CASCADE;
        ''')
        if tablename == "passend_bij_uw_gedrag":
            bsql.do_query(conn, cur, f'''
                CREATE TABLE {tablename} (profileID varchar(255) NOT NULL, productsID1 varchar(255) NOT NULL, productsID2 varchar(255) NOT NULL, productsID3 varchar(255) NOT NULL, productsID4 varchar(255) NOT NULL, productsID5 varchar(255) NOT NULL);
            ''')
            bsql.do_query(conn, cur, f'''
                ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305529 FOREIGN KEY (profileID) REFERENCES profiles (ID);
                ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305533 FOREIGN KEY (productsID1) REFERENCES products (ID);
                ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305534 FOREIGN KEY (productsID2) REFERENCES products (ID);
                ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305535 FOREIGN KEY (productsID3) REFERENCES products (ID);
                ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305536 FOREIGN KEY (productsID4) REFERENCES products (ID);
                ALTER TABLE {tablename} ADD CONSTRAINT FKmost_popul305537 FOREIGN KEY (productsID5) REFERENCES products (ID);
            ''')
    bsql.copy_files_to_tables(conn, cur, tablenames)


tablenames = ["passend_bij_uw_gedrag"]
create_fill_tables(tablenames)
