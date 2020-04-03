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
    # query_2 = '''
    #             select s.id, count(p.id)
    #             from products as p, orders as o, sessions as s
    #             where p.id = o.productid
    #             and o.sessionsid = s.id
    #             group by s.id
    #             having
    # 	            count(p.id) > 1;
    #          '''
    # data_2 = bsql.select_data(cur, query_2)
    counter = 0
    list = []
    for row in data_1[10000]:
        counter += 1
        if counter % 100 == 0:
            print(counter)
        ses_id = row[0]
        products_in_order = row[1]
        list.append([ses_id, products_in_order])
    # print(data_1)
    print('return')
    return data_1[:10000]

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
                list_tijdelijk = []
                for i in dictionary[item]:
                    if producten != i:
                        list_tijdelijk.append(i)
                new_dict.update({producten : list_tijdelijk})

            elif producten in new_dict:
                switch = new_dict[producten]
                for gegeven in dictionary[item]:
                    if gegeven != producten:
                        switch.append(gegeven)
                new_dict.update({producten: switch})

    return new_dict

def sorter(dictionary):
    new_dict = dict({})
    for element in dictionary:
        value = dictionary.get(element)
        sorted_value = top_frequent_items(value, len(value))
        new_dict.update({element:sorted_value})
    return new_dict

def most_frequent_item(List):
    """Count most frequent item in list"""
    counter = 0
    freq_item = List[0]
    for item in List:
        current_freq = List.count(item)
        if current_freq > counter:
            counter = current_freq
            freq_item = item
    return freq_item

def top_frequent_items(List, top_number):
    """Makes a list of the most frequent items in list"""
    if len(List) == top_number:
        return List
    list_frequent_items = []
    for top in range(top_number):
        most_frequent = most_frequent_item(List)
        list_frequent_items.append(most_frequent)
        for remove_item in range(List.count(most_frequent)):
            if most_frequent in List:
                List.remove(most_frequent)
    return list_frequent_items

def filter(dictionary):
    new_dict = dict({})
    for element in dictionary:
        value = dictionary.get(element)
        if len(value) == 5:
            new_dict.update({element: value})
        elif len(value) > 5:
            new_dict.update({element : value[:6]})
    return new_dict


def csv_creator(dictionary):
    print(dictionary)
    with open('combineert_goed_met.csv', 'w', newline="") as csv_file:
        writer = csv.writer(csv_file)
        for key, value in dictionary.items():
            writer.writerow([key, value])
    csv_file.close()



a = cleaner(profile_products(combineert_goed_met()))
print('clean is done.......................')
b = product_counter(a)
print('teller is done.....................')
sorted = sorter(b)
print('sorted', sorted)
print('sorted is done.........................')
gefilterd = filter(sorted)
print('filter', gefilterd)
print('filter is done.......................')
csv_creator(gefilterd)
print('csv creatie is done....................')