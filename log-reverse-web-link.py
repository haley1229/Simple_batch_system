import random
import string
import os
from random import randint


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "www." + ''.join(random.choice(letters) for i in range(stringLength)) + ".com"


def generate_random_logs(num_files, num_lines, num_strings):
    os.chdir("weblink/")
    strings = []
    count_string = []
    for i in range(num_strings):
        strings.append(randomString())
        count_string.append([])
    
    for i in range(num_files):
        f = open(f'link{i}.log', 'w+')
        for j in range(num_lines):

            index1 = randint(0, num_strings) - 1
            index2 = randint(0, num_strings) - 1

            count_string[index1].append(strings[index2])

            strline = strings[index1] + ' ' + strings[index2] + '\n'
            f.write(strline)

    f = open('correctreduce.log', 'w+')
    for i in range(len(strings)):
        f.write(f'{strings[i]}:   {count_string[i]}\n')

generate_random_logs(num_files=10, num_lines=20000, num_strings=1000)






# f = open('access-copy.log')


# f1 = open('access1.log', 'a')
# f2 = open('access2.log', 'a')
# f3 = open('access3.log', 'a')
# f4 = open('access4.log', 'a')
# f5 = open('access5.log', 'a')
# f6 = open('access6.log', 'a')
# f7 = open('access7.log', 'a')
# f8 = open('access8.log', 'a')
# f9 = open('access9.log', 'a')
# f10 = open('access10.log', 'a')

# f11 = open('access11.log', 'a')
# f12 = open('access12.log', 'a')

# f = open('wiki.log')

# f =  open('wiki.log', 'r', errors='replace')

# f1 = open('wiki1.log', 'a+')
# f2 = open('wiki2.log', 'a+')
# f3 = open('wiki3.log', 'a+')
# f4 = open('wiki4.log', 'a+')
# f5 = open('wiki5.log', 'a+')
# f6 = open('wiki6.log', 'a+')
# f7 = open('wiki7.log', 'a+')
# f8 = open('wiki8.log', 'a+')
# f9 = open('wiki9.log', 'a+')
# f10 = open('wiki10.log', 'a+')

# f11 = open('wiki11.log', 'a+')
# f12 = open('wiki12.log', 'a+')

# number = 0
# for x in f.readlines():
#     number = number + 1
#     if (number >= 0 and number < 150000):
#         f1.write(x)
#     elif (number >= 150000 and number < 300000):
#         f2.write(x)
#     elif (number >= 300000 and number < 450000):
#         f3.write(x)
#     elif (number >= 450000 and number < 600000):
#         f4.write(x)
#     elif (number >= 600000 and number < 750000):
#         f5.write(x)
#     elif (number >= 750000 and number < 950000):
#         f6.write(x)
#     elif (number >= 900000 and number < 1050000):
#         f7.write(x)
#     elif (number >= 1050000 and number < 1200000):
#         f8.write(x)
#     elif (number >= 1200000 and number < 1350000):
#         f9.write(x)
#     elif (number >= 1350000 and number < 1500000):
#         f10.write(x)
#     elif (number >= 1500000 and number < 1600000):
#         f11.write(x)
#     elif (number >= 1600000 and number < 1869880):
#         f12.write(x)
# f.close()
# f1.close()
# f2.close()
# f3.close()
# f4.close()
# f5.close()
# f6.close()
# f7.close()
# f8.close()
# f9.close()
# f10.close()
# f11.close()
# f12.close()
