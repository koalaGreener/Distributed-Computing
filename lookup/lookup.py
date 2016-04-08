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

if __name__ == '__main__':
     readTheFile("data/king.txt")
     readTheFile("data/macbeth.txt")
     readTheFile("data/juliet.txt")

