package CEG;

import CEG.Optimistic.Mode;
import CEG.Parallel.OptimisticRunnable;
import CEG.Parallel.PessimisticRunnable;
import Common.Query;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class CardinalityEstimation {
    enum EstMethod {
        OPT,
        PESS;

        public static EstMethod getEstMethod(String method) {
            switch (method) {
                case "opt":
                    return OPT;
                case "pess":
                    return PESS;
                default:
                    System.err.println("ERROR: unrecognized est method");
                    System.err.println("   method: " + method);
                    return null;
            }
        }
    }

    private static String toResultString(Query query, Double[] est) {
        if (est.length == 3) {
            return query.toString() + "," + est[0] + "," + est[1] + "," + est[2];
        } else {
            return query.toString() + "," + est[0] + "," + est[1];
        }
    }

    private static String toResultString(Query query, BigInteger est) {
        return query.toString() + "," + est;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("method: " + args[0]);
        System.out.println("queryFile: " + args[1]);
        System.out.println("queryVList: " + args[2]);
        System.out.println("startLen: " + args[3]);
        System.out.println("budget: " + args[4]);

        EstMethod method = EstMethod.getEstMethod(args[0]);
        List<Query> queries = Query.readQueries(args[1]);
        String queryVList = args[2];
        Integer startLen = Integer.parseInt(args[3]);
        Integer budget = Integer.parseInt(args[4]);

        int numQueries = queries.size();
        double progress = 0;
        StopWatch watch = new StopWatch();
        watch.start();

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("estimation.csv"));

        List<Thread> threads = new ArrayList<>();
        Thread thread;

        switch (method) {
            case OPT:
                System.out.println("pcatFile: " + args[5]);
                Mode mode = args.length == 7 ? Mode.MIN_MAX : Mode.MIN_MAX_AVG;
                if (args.length == 7) {
                    System.out.println("mode: MIN_MAX");
                }
                System.out.println();

                List<OptimisticRunnable> optRunnables = new ArrayList<>();

                Optimistic opt = new Optimistic(queryVList, startLen, args[5]);
                for (Query query : queries) {
                    OptimisticRunnable runnable = new OptimisticRunnable(opt, query, budget, mode);
                    optRunnables.add(runnable);
                    thread = new Thread(runnable);
                    threads.add(thread);
                    thread.start();
                }

                for (Thread t : threads) {
                    t.join();
                    progress += 100.0 / numQueries;
                    System.out.print("\rEstimating: " + (int) progress + "%");
                }

                for (OptimisticRunnable r : optRunnables) {
                    resultWriter.write(toResultString(r.getQuery(), r.getResults()) + "\n");
                }

                break;
            case PESS:
                System.out.println("pcatFile: " + args[5]);
                System.out.println("pcatMaxDegFile: " + args[6]);
                System.out.println();

                List<PessimisticRunnable> pessRunnables = new ArrayList<>();

                Pessimistic pess = new Pessimistic(queryVList, startLen, args[5], args[6]);
                for (Query query : queries) {
                    PessimisticRunnable runnable = new PessimisticRunnable(pess, query, budget);
                    pessRunnables.add(runnable);
                    thread = new Thread(runnable);
                    threads.add(thread);
                    thread.start();
                }

                for (Thread t : threads) {
                    t.join();
                    progress += 100.0 / numQueries;
                    System.out.print("\rEstimating: " + (int) progress + "%");
                }

                for (PessimisticRunnable r : pessRunnables) {
                    resultWriter.write(toResultString(r.getQuery(), r.getResults()) + "\n");
                }
                break;
        }
        resultWriter.close();

        watch.stop();
        System.out.println("\nEstimating: " + (watch.getTime() / 1000.0) + " sec");
    }
}
