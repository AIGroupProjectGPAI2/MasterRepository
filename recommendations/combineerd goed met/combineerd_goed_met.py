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
        break
    print(sorted_combis)
    with open('test.json', 'w') as tf:
        tf.write(json.dumps(sorted_combis, indent=4))


order_dict_in_dict()
