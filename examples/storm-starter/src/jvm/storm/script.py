import numpy as np
import matplotlib.pyplot as plt
from prettytable import PrettyTable

def main(keyword):
    # acks_to_time = dict()
    acks_second = []
    acks_second_time = []
    ack_lines = [line for line in open('word_count_12-51.log', 'r') if keyword in line]
    for line in ack_lines:
        line = line[line.index(keyword):]
        words = line.split()
        acks_second.append(words[1])
        acks_second_time.append(words[2])
    print(acks_second)
    print(acks_second_time)
    # print("avg:" + str(sum(float(x) for x in acks_second)/len(acks_second)))
    # plt.plot(acks_second_time, acks_second, linestyle='-')
    # plt.title(keyword)
    # plt.show()
    # plt.figure()


def parallellism_stuff(keyword):
    header_list = []
    table_data = []
    table_data_temp = []
    temp_2 = []
    acks_second = []
    acks_second_avg = []
    # ack_lines = [line for line in open('word_count_12-51.log', 'r') if keyword in line]
    for line in open('word_count_12-51.log','r'):
        if keyword in line:
            line = line[line.index(keyword):]
            words = line.split()
            header_list.append(words[1])
            table_data_temp.append(words[2])
            if acks_second:
                acks_second_avg.append(sum(float(x) for x in acks_second)/len(acks_second))
                acks_second = []
        elif "acksPerSecond" in line:
            line = line[line.index("acksPerSecond"):]
            words = line.split()
            acks_second.append(words[1])
    header_set = list(set(header_list))
    count = 0
    for elem in header_list:
        if not header_set:
            break;
        else:
            header_set.remove(elem)
            count += 1
    header_list = header_list[:count]
    header_list.append("acks_second_avg")
    table = PrettyTable(header_list)
    i = 0
    avg_count = 1
    for elem in table_data_temp:
        temp_2.append(elem)
        i += 1
        if (i % (len(header_list)-1)) == 0:
            # table_data.append(temp_2)
            table.add_row(temp_2 + [acks_second_avg[avg_count%len(acks_second_avg)]])
            avg_count += 1
            temp_2 = []
    print(header_list)
    print(acks_second_avg)
    # print(table_data)
    print(table)
    # print tabulate(table_data,headers=header_list,tablefmt='orgtbl')
    # plt.plot(acks_second_time, acks_second, label=keyword, linestyle='-')
    # plt.show()

if __name__ == "__main__":
    main("acksPerSecond")
    main("cpuUsage")
    parallellism_stuff("parallelism")