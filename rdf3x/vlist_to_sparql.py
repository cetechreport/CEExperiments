import sys
  
v = set()
edges = []
with open(sys.argv[1], 'r') as f:
    for line in f:
        splitted = line.split(",")
        vertices = splitted[0].split(";")
        labels = splitted[1].split("->")
        for i in range(len(vertices)):
            v1 = vertices[i].split("-")[0]
            v2 = vertices[i].split("-")[1]
            v.add(v1)
            v.add(v2)
            edges.append([v1, labels[i], v2])
f.close()

s = "select "
for i in range(len(v)):
    s += "?v" + str(i) + " "

s += "where { \n"
for edge in edges:
    s += "  ?v" + edge[0] + " <" + edge[1] + "> ?v" + edge[2] + " .\n"
s += "}\n"

with open(sys.argv[2], 'w') as f:
    f.write(s)
f.close()
