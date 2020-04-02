import csv
import thebestsql as bsql

conn = bsql.get_connection("3621", "postgres")
cur = bsql.get_cursor(conn)

def combineert_goed_met():
    orders_cats = {}
    orders = {}
    query = '''
            select s.id, count(p.id)
            from products as p, orders as o, sessions as s
            where p.id = o.productid
            and o.sessionsid = s.id
            group by s.id
            having
	            count(p.id) > 1;
         '''
    data = bsql.select_data(cur, query)


    for row in data:
        ses_id = row[0]
        products_in_order = row[1]
        print(ses_id, products_in_order)

    # for row in data:
    #     cat = row[1]
    #     product_id = row[0]
    #     product_count = row[2]
    #     if cat not in orders_cats:
    #         orders_cats[cat] = {}
    #     if product_id in orders_cats[cat]:
    #         orders_cats[cat][product_id] += product_count
    #     else:
    #         orders_cats[cat][product_id] = product_count
    #
    #     if product_id in orders:
    #         orders[product_id] += product_count
    #     else:
    #         orders[product_id] = product_count
    # cats_popular = {}
    # for k in orders_cats.keys():
    #     orders_cats[k] = sorted(orders_cats[k].items(), key=lambda x: x[1], reverse=True)
    # orders = sorted(orders.items(), key=lambda x: x[1], reverse=True)
    # return orders, orders_cats

print(combineert_goed_met())