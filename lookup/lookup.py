def readTheFile(filename):
    result = [0] * 20
    with open(filename) as infile:
        for line in infile:
            line = line.strip("kingmacbethjuliet)])")
            lineeach = line.split("), (")
            for line in lineeach:
                #print(line)
                result[int(line[len(line) - 1])] += 1
        print(filename)
        print(result)

'''import the data that contains the spam'''
if __name__ == '__main__':
     readTheFile("/Users/HUANGWEIJIE/Desktop/king.txt")
     readTheFile("/Users/HUANGWEIJIE/Desktop/macbeth.txt")
     readTheFile("/Users/HUANGWEIJIE/Desktop/juliet.txt")

