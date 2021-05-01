
import RDF3X.HistogramType;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;

public class SimplifyRDF {

    public static void main(String[] args) throws Exception {
        String sourceHdtFileName = args[0];
        String destNtFileName = args[1];

        HashMap<HistogramType, BufferedWriter> type2writer = new HashMap<>();

//        for (HistogramType histogramType : HistogramType.values()) {
//            type2writer.put(histogramType, new BufferedWriter(new FileWriter(destNtFileName +
//                histogramType + ".csv")));
//        }
        type2writer.put(HistogramType.SP, new BufferedWriter(new FileWriter(destNtFileName)));

        HDT hdt = HDTManager.loadHDT(sourceHdtFileName, null);

        String triple;
        IteratorTripleID itTripleId = hdt.getTriples().searchAll();
        while (itTripleId.hasNext()) {
            TripleID tripleID = itTripleId.next();

            triple = tripleID.getSubject() + "," + tripleID.getPredicate() + "," +
                tripleID.getObject() + "\n";
            type2writer.get(HistogramType.SP).write(triple);

//            triple = tripleID.getSubject() + "," + tripleID.getObject() + "," +
//                tripleID.getPredicate() + "\n";
//            type2writer.get(HistogramType.SO).write(triple);
//
//            triple = tripleID.getPredicate() + "," + tripleID.getSubject() + "," +
//                tripleID.getObject() + "\n";
//            type2writer.get(HistogramType.PS).write(triple);
//
//            triple = tripleID.getPredicate() + "," + tripleID.getObject() + "," +
//                tripleID.getSubject() + "\n";
//            type2writer.get(HistogramType.PO).write(triple);
//
//            triple = tripleID.getObject() + "," + tripleID.getSubject() + "," +
//                tripleID.getPredicate() + "\n";
//            type2writer.get(HistogramType.OS).write(triple);
//
//            triple = tripleID.getObject() + "," + tripleID.getPredicate() + "," +
//                tripleID.getSubject() + "\n";
//            type2writer.get(HistogramType.OP).write(triple);
        }

//        // close the writers
//        for (HistogramType histogramType : HistogramType.values()) {
//            type2writer.get(histogramType).close();
//        }

        type2writer.get(HistogramType.SP).close();
    }
}
