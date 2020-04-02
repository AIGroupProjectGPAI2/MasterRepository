import csv
nms = [["hoi", "hoi", "hoi"], [7, 8, 9, 10, 11, 12]]
f = open('numbers2.csv', 'w')
with f:
    writer = csv.writer(f)

    for row in nms:
        writer.writerow(row)