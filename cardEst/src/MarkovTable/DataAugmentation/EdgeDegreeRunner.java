package MarkovTable.DataAugmentation;

public class EdgeDegreeRunner {
    public static void main(String[] args) throws Exception {
        EdgeDegrees edgeDegrees = new EdgeDegrees(args[0]);
        edgeDegrees.getAllTriples();
        edgeDegrees.removeHighHighDegreeNodesBridge(Integer.parseInt(args[1]));
    }
}
