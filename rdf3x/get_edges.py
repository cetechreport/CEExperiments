import sys
edges = []

with open(sys.argv[1] , 'r') as f:
    for line in f:
        # if line.startswith('e'):
        #     edge = line.strip().split()
        #     edges.append(edge[3])
        if "<" in line:
            edge = line[line.find("<") + 1:line.find(">")]
            edges.append(edge)
f.close()

with open(sys.argv[2] , 'w') as f:
    for edge in edges:
        f.write(edge + ' ')
    f.write("\n")
f.close()
