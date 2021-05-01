package MarkovTable.DataAugmentation;

public class NodeDegreeSaver {
    public static void main(String[] args) throws Exception {
        NodeDegrees nodeDegrees = new NodeDegrees(args[0]);
        nodeDegrees.listDegrees();
    }
}
