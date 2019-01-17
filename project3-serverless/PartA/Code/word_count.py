import os
import sys

def count(loc_i,loc_o):
    file_list = [f for f in os.listdir(loc_i) if os.path.isfile(os.path.join(loc_i, f))]

    text = []
    word_count = {}
    #print(len(file_list))
    for i in range(len(file_list)):
        f = open(loc_i+"/"+file_list[i],'r')
        text.append(f.readlines())
        f.close()

    for i in range(len(file_list)):
        for j in range(len(text[i])):
            tweet = text[i][j].split(" ")
            for i in range(len(tweet)):
                word = tweet[i].strip()
                if(word != ''):
                    if(word in word_count):
                        word_count[word] += 1
                    else:
                        word_count[word] = 1

    l = [(value,key) for (key,value) in word_count.items()]
    l.sort(key=lambda x: x[1])
    l.sort(key=lambda x: x[0], reverse=True)

    f = open(loc_o,'w+')
    for i in range(len(l)):
        f.write(str(l[i][1])+"\t"+str(l[i][0])+"\n")
    f.close()

count(sys.argv[1],sys.argv[2])
