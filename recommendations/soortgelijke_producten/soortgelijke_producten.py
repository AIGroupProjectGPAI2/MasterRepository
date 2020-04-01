import thebestsql as bsql
import csv, random

conn = bsql.get_connection("Floris09", "huwebshop")
cur = bsql.get_cursor(conn)

def sort_all_products():
    cnt = 0
    query = '''
        SELECT p.id, c.category, c.subcategory, c.subsubcategory, d.id
        FROM products as p, categories as c, doelgroepen as d
        WHERE c.id = p.categoriesid
        AND   d.id = p.doelgroepenid'''
    data = bsql.select_data(cur, query)
    ldata = len(data)
    print("Got the data")
    with open('soort_gelijke_producten.csv', 'w', newline='', encoding='utf-8') as sgp:
        sgp_fieldnames = ["productid", "productid1", "productid2", "productid3", "productid4", "productid5"]
        sgp_writer = csv.DictWriter(sgp, fieldnames=sgp_fieldnames)
        sgp_writer.writeheader()
        print("Products 2 loop trhuuuuuuuu (ongeveer): ", ldata * ldata)
        for row in data:
            # Values 2 compare 2
            pid = row[0]
            cat = row[1]
            scat = row[2]
            sscat = row[3]
            did = row[4]
            sscd = []
            ssc = []
            scd = []
            sc = []
            cd = []
            c = []
            d = []
            n = []
            for r in data:
                if r[0] != pid:
                    if r[4] == did:
                        if r[3] == sscat:
                            sscd.append(r[0])
                        elif r[2] == scat:
                            scd.append(r[0])
                        elif r[1] == cat:
                            cd.append(r[0])
                        else:
                            d.append(r[0])
                    else:
                        if r[3] == sscat:
                            ssc.append(r[0])
                        elif r[2] == scat:
                            sc.append(r[0])
                        elif r[1] == cat:
                            c.append(r[0])
            all_lists = [d, c, cd, sc, scd, ssc, sscd]
            for l in all_lists:
                if len(l) >= 5:
                    selection = random.sample(l, 5)
            sgp_writer.writerow(
                {
                    "productid": pid,
                    "productid1": selection[0],
                    "productid2": selection[1],
                    "productid3": selection[2],
                    "productid4": selection[3],
                    "productid5":selection[4]
                }
            )
            cnt += 1
            if cnt % 100 == 0:
                print(cnt, '/', ldata)
sort_all_products()
