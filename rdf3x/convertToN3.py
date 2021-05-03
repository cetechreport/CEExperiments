import sys
  
src_file = sys.argv[1]
dest_file = sys.argv[2]
w = []

with open(src_file, "r") as f:
    for line in f:
        vertices = line.split(',')
        w.append("<" + vertices[0] + "> <" + vertices[1] + "> <" + vertices[2][:-1] + "> ." + "\n")
    f.close()

with open(dest_file, "w") as f:
    for line in w:
        f.write(line)
f.close()
