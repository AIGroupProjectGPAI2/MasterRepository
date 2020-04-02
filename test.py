lst = [1,2,2,3,3,3,4,4,4,4,5,5,5,5,5]

def most_frequent_item(List):
    counter = 0
    freq_item = List[0]
    for item in List:
        current_freq = List.count(item)
        if current_freq > counter:
            counter = current_freq
            freq_item = item
    return freq_item
def top_frequent_items(List, top_number):
    list_frequent_items = []
    for top in range(top_number):
        most_frequent = most_frequent_item(List)
        list_frequent_items.append(most_frequent)
        for remove_item in range(List.count(most_frequent)):
            List.remove(most_frequent)
    return list_frequent_items


print(top_frequent_items(lst, 2))



