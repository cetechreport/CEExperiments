package TrueCardinality;

public class LargeBenchmarkPatternFinder extends PatternFinderRunner {
    public LargeBenchmarkPatternFinder(String graphFile) throws Exception {
        super(graphFile);
    }

    protected void run() throws Exception {
        findPattern("201", 201, "0-1;0-2;0-3;0-4;0-5;0-6");
        findPattern("202", 202, "0-1;0-2;0-3;0-4;0-5;5-6");
        findPattern("203", 203, "0-1;0-2;0-3;3-4;4-5;4-6");
        findPattern("204", 204, "0-1;0-2;0-3;3-4;4-5;5-6");
        findPattern("205", 205, "0-1;0-3;2-3;2-4;4-5;5-6");

        findPattern("206", 206, "0-1;0-2;0-3;0-4;0-5;0-6;0-7");
        findPattern("207", 207, "0-1;0-2;0-3;0-4;0-5;0-6;6-7");
        findPattern("208", 208, "0-1;0-2;0-3;0-4;4-5;5-6;5-7");
        findPattern("209", 209, "0-1;0-2;0-3;0-4;4-5;5-6;6-7");
        findPattern("210", 210, "0-1;1-3;2-3;2-4;4-6;5-6;6-7");
        findPattern("211", 211, "0-1;0-3;2-3;2-5;4-5;4-6;6-7");

        findPattern("212", 212, "0-2;1-2;2-3;2-4;2-5;2-6;2-7;2-8");
        findPattern("213", 213, "0-1;0-2;0-3;0-4;0-5;0-6;0-7;7-8");
        findPattern("214", 214, "0-1;0-2;0-3;0-4;0-5;0-6;6-7;7-8");
        findPattern("215", 215, "0-1;0-2;0-3;0-4;4-5;5-6;6-7;6-8");
        findPattern("216", 216, "0-2;1-2;1-3;3-5;4-5;4-6;4-7;4-8");
        findPattern("217", 217, "0-1;1-2;2-4;3-4;3-6;5-6;5-7;5-8");
        findPattern("218", 218, "0-1;0-3;2-3;2-5;4-5;4-6;6-7;7-8");

        // hetionet
        findPattern("219", 219, "0-3;1-3;2-3;3-4;3-5;3-6"); // 201
        findPattern("220", 220, "0-3;1-3;2-3;3-4;3-5;3-6;3-7"); // 206
        findPattern("221", 221, "0-3;1-3;2-3;3-4;3-5;3-6;6-7"); // 207
        findPattern("222", 222, "0-2;1-2;2-3;2-4;4-6;5-6;5-7"); // 209
        findPattern("223", 223, "0-4;1-4;2-4;3-4;4-5;4-6;4-7;4-8"); // 212
        findPattern("224", 224, "0-4;1-4;2-4;3-4;4-5;4-6;4-7;7-8"); // 213
        findPattern("225", 225, "0-2;1-2;1-6;3-6;4-6;5-6;6-7;6-8"); // 214
        findPattern("226", 226, "0-2;1-2;2-4;3-4;3-7;5-7;6-7;7-8"); // 215
        findPattern("227", 227, "0-1;0-3;2-3;2-5;4-5;4-7;6-7;7-8"); // 217
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println();

        PatternFinderRunner runner = new LargeBenchmarkPatternFinder(args[0]);
        runner.run();
    }
}
