package MarkovTable;

import java.io.File;

public class MTSerializer {
    public static void main(String[] args) throws Exception {
        File fileOrDir = new File(args[0]);
        if (fileOrDir.isDirectory()) {
            String[] partitionFiles = fileOrDir.list();
            for (int i = 0; i < partitionFiles.length; ++i) {
                MarkovTable markovTable = new MarkovTable(
                    args[0] + partitionFiles[i], Integer.parseInt(args[1]), false,true
                );

                markovTable.save(args[0] + partitionFiles[i].split("\\.")[0] + "_MT.csv");
            }
        } else {
            MarkovTable markovTable = new MarkovTable(
                args[0], Integer.parseInt(args[1]), false,true
            );
            markovTable.save(args[2]);
        }
    }
}
