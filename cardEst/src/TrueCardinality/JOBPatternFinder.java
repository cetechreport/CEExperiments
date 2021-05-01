package TrueCardinality;

public class JOBPatternFinder extends PatternFinderRunner {
    public JOBPatternFinder(String graphFile) throws Exception {
        super(graphFile);
    }

    protected void run() throws Exception {
        findPattern("101", 101, "0-2;1-2;1-3");
        findPattern("102", 102, "0-2;1-2;2-3");
        findPattern("103", 103, "0-1;1-2;1-3");
        findPattern("104", 104, "0-3;1-3;2-3");
        findPattern("105", 105, "0-2;1-2;2-3;2-4");
        findPattern("106", 106, "0-1;0-2;0-3;3-4");
        findPattern("107", 107, "0-1;0-4;2-4;3-4");
        findPattern("108", 108, "0-1;0-3;2-3;3-4");
        findPattern("109", 109, "0-3;1-3;2-3;3-4;3-5");
        findPattern("110", 110, "0-1;1-2;1-4;3-4;4-5");
        findPattern("111", 111, "0-1;0-2;0-5;3-5;4-5;5-6");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println();

        PatternFinderRunner runner = new JOBPatternFinder(args[0]);
        runner.run();
    }
}
