import csv
import thebestsql as bsql

conn = bsql.get_connection("3621", "postgres")
cur = bsql.get_cursor(conn)

def combineert_goed_met():
    orders_cats = {}
    orders = {}
    query_1 = '''
            select sessionsid, productid from orders
         '''
    data_1 = bsql.select_data(cur, query_1)
    query_2 = '''
                select s.id, count(p.id)
                from products as p, orders as o, sessions as s
                where p.id = o.productid
                and o.sessionsid = s.id
                group by s.id
                having
    	            count(p.id) > 1;
             '''
    data_2 = bsql.select_data(cur, query_2)
    list = []
    counter = 0
    for row in data_1[:1000]:
        counter += 1
        if counter % 100 == 0:
            print(counter)
        ses_id = row[0]
        products_in_order = row[1]
        list.append([ses_id, products_in_order])
    # print(data_1)

    return data_1[:1000]




def profile_products(data):
    profile_products_list = dict({})
    for record in data:
        record_in_dic = False
        for profiles in profile_products_list:
            if record[0] == profiles:
                products = [record[1]]
                lst = list(profile_products_list.get(profiles)) + products
                profile_products_list.update({profiles: lst})
                record_in_dic = True
                break
        if not record_in_dic:
            products = [record[1]]
            profile_products_list.update({record[0]: products})
    return profile_products_list


def cleaner(dictionary):

    schone_dict = dict({})
    for items in dictionary:
        products = dictionary.get(items)
        if len(products) > 1:
            schone_dict.update({items : products})

    return schone_dict


def product_counter(dictionary):
    new_dict = dict({})
    for item in dictionary:
        for producten in dictionary[item]:
            if producten not in new_dict:
                print('')
                list_tijdelijk = []
                for i in dictionary[item]:
                    print(i, producten)
                    if producten != i:
                        list_tijdelijk.append(i)
                    print(list_tijdelijk)
                new_dict.update({producten : list_tijdelijk})
            if producten in new_dict:
                switch = new_dict[producten]
                print(switch, 'switch')
                switch.append(dictionary[item])
                new_dict.update({producten: switch})

    print(new_dict)

# print(combineert_goed_met())
# print(profile_products(combineert_goed_met()))
a = cleaner(profile_products(combineert_goed_met()))
product_counter(a)