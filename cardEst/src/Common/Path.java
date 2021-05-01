package Common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Path {
    private List<Integer> edgeLabelList;
    private List<Integer> srcVertexList;
    private List<Integer> fromPath;
    private List<Integer> toPath;

    public Path(List<Integer> edgeLabelList) {
        this.edgeLabelList = new ArrayList<>(edgeLabelList);
        this.fromPath = new ArrayList<>();
        this.toPath = new ArrayList<>();
        this.srcVertexList = new ArrayList<>();
        for (int i = 0; i < edgeLabelList.size(); ++i) {
            this.srcVertexList.add(-1);
        }
    }

    public Path(String edgeListCsvString) {
        this.edgeLabelList = new ArrayList<>();
        String[] labels = edgeListCsvString.split(",");
        for (int i = 0; i < labels.length; ++i) {
            this.edgeLabelList.add(Integer.parseInt(labels[i]));
        }
    }

    public Path(List<Integer> edgeLabelList, List<Integer> someList, String type) {
        this.edgeLabelList = new ArrayList<>(edgeLabelList);
        if (type.contains("fromPath")) {
            this.fromPath = someList;
            this.toPath = new ArrayList<>();
            this.srcVertexList = new ArrayList<>();
            for (int i = 0; i < edgeLabelList.size(); ++i) {
                this.srcVertexList.add(-1);
            }
        } else if (type.contains("toPath")) {
            this.fromPath = new ArrayList<>();
            this.toPath = someList;
            this.srcVertexList = new ArrayList<>();
            for (int i = 0; i < edgeLabelList.size(); ++i) {
                this.srcVertexList.add(-1);
            }
        } else {
            this.fromPath = new ArrayList<>();
            this.toPath = new ArrayList<>();
            this.srcVertexList = new ArrayList<>(someList);
        }
    }

    public Path(Path anotherPath) {
        this.edgeLabelList = new ArrayList<>(anotherPath.edgeLabelList);
//        this.srcVertexList = new ArrayList<>(anotherPath.srcVertexList);
    }

    public void append(int label) {
        edgeLabelList.add(label);
    }

    public void appendAll(List<Integer> labels) {
        edgeLabelList.addAll(labels);
    }

    public Path subPath(int begin, int end) {
        return new Path(edgeLabelList.subList(begin, end));
    }

    public List<Integer> getEdgeLabelList() {
        return edgeLabelList;
    }

    public List<Integer> getSrcVertexList() {
        return srcVertexList;
    }

    public List<Integer> getFromPath() {
        return fromPath;
    }

    public void setSrcVertexList(List<Integer> srcVertexList) {
        this.srcVertexList = srcVertexList;
    }

    public int length() {
        return edgeLabelList.size();
    }

    public Path getPrefix(int length) {
        return new Path(edgeLabelList.subList(0, length), srcVertexList.subList(0, length), "src");
    }

    public Path getPrefix() {
        return new Path(edgeLabelList.subList(0, this.length() - 1));
    }

    public Path getSuffix(int length) {
        return new Path(edgeLabelList.subList(edgeLabelList.size() - length, edgeLabelList.size()));
    }

    public boolean containsSuffix(Path path) {
        if (path.length() > this.length()) return false;

        int lenDiff = this.length() - path.length();
        for (int i = 0; i < path.length(); ++i) {
            if (!path.edgeLabelList.get(i).equals(this.edgeLabelList.get(i + lenDiff))) return false;
        }

        return true;
    }

    public Path[] getAllSubpaths(int length) {
        Path[] subpaths = new Path[edgeLabelList.size() - length + 1];
        for (int i = 0; i <= edgeLabelList.size() - length; ++i) {
            subpaths[i] = new Path(
                edgeLabelList.subList(i, i + length),
                srcVertexList.subList(i, i + length),
                "src"
            );
        }
        return subpaths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Path path = (Path) o;

        return edgeLabelList.equals(path.edgeLabelList);
//               srcVertexList.equals(path.srcVertexList) &&
//               fromPath.equals(path.fromPath) &&
//               toPath.equals(path.toPath);
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Integer edgeLabel : edgeLabelList) {
            result += edgeLabel;
        }
        return result;
    }

    @Override
    public String toString() {
        String str = "";
        for (int i = 0; i < edgeLabelList.size(); ++i) {
            if (srcVertexList.get(i) >= 0) {
                if (i < edgeLabelList.size() - 1) {
                    str += srcVertexList.get(i) + "-" + edgeLabelList.get(i) + "->";
                } else {
                    str += srcVertexList.get(i) + "-" + edgeLabelList.get(i) + "->*";
                }
            } else {
                if (i == 0) {
                    str += fromPath.toString();
                }
                if (i < edgeLabelList.size() - 1) {
                    str += "*-" + edgeLabelList.get(i) + "->";
                } else {
                    str += "*-" + edgeLabelList.get(i) + "->*";
                }
                if (i == edgeLabelList.size() - 1) {
                    str += toPath.toString();
                }
            }
        }

        return str;
    }

    public String toCsv() {
        String str = "";
        for (int i = 0; i < edgeLabelList.size(); ++i) {
            if (i < edgeLabelList.size() - 1) {
                str += edgeLabelList.get(i) + ",";
            } else {
                str += edgeLabelList.get(i);
            }
        }

        return str;
    }

    public String toSimpleString() {
        String str = "";
        for (int i = 0; i < edgeLabelList.size(); ++i) {
            if (i < edgeLabelList.size() - 1) {
                str += edgeLabelList.get(i) + "->";
            } else {
                str += edgeLabelList.get(i);
            }
        }
        return str;
    }
}
