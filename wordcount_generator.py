def evaluate():
    r = open("wordcount.txt",'r')
    for line in r:
        line = line.strip('\n')
        tuple = eval(line)
        print(tuple[0])
def gen():
    w = open("wordcount.txt", 'w')
    list = ["applepie", 'app', 'beaware', 'concatanate', 'deathroll', 'extravaganza', 'fatality', 'gorgeous']
    for i in range(0, 10):
        for item in list:
            w.write(str((item,1))+'\n')
            #w.write(item + '\n')

if __name__ == "__main__":
    gen()