# #!/usr/bin/env python
# # _*_ coding:utf-8 _*_

# file path
path1 = "F://Code/big_data/abword.txt"
path2 = "F://Code/big_data/strategy2.txt"
path3 = "F://Code/big_data/abstract2.txt"
path4 = "F://Code/big_data/result.txt"

# use function(intersection) to get a list, which
# includes words both in UK strategy and DOI:Abstract
s1 = set()
s2 = set()
f1 = open(path1, "r", encoding='utf-8')
file1 = f1.readlines()
for i in file1:
    i = i.replace('\n', " ")
    s1.add(i)
f2 = open(path2, "r", encoding='utf-8')
file2 = f2.readlines()
for i in file2:
    i = i.replace('\n', " ")
    s2.add(i)
same = set(s1).intersection(s2)
same1 = list(same)

# spilt the file to two list,one includes all doi and
# another one includes all abstract words.Then construct
# an one-key multi-values dictionary(one key means the doi,
# and multi-values means the abstract words)
y = []
doi = []
wd = []
he = []
DOI = {}
f3 = open(path3, 'r', encoding="utf-8")
file3 = f3.readlines()
for line in file3:
    y.extend(line.split(":"))
for i in y[::2]:
    doi.append(i)
for j in y[1::2]:
    j = j.replace("\n", ' ')
    wd.append(j)
he = zip(doi, wd)
he = list(he)
for k, v in he:
    if k in DOI:
        DOI[k].append(v)
    else:
        DOI[k] = [v]

outfile = open(path4, "w", encoding='utf-8')
outfile.write("result：\n")

# use for loop to traversing the dictionary and get the
# same words appear in strategy words and abstract words
for k, v in DOI.items():
    sum = 0
    for item in v:
        sum += 1
        if item in same1:
            print(k, ":", item)
            outfile.write(k + ":" + item + "\n")
    print(sum)
    outfile.write(str(sum)+"\n")
