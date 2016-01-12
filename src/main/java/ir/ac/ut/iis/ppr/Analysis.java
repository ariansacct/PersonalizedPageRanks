package ir.ac.ut.iis.ppr;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class Analysis {

    static int sum_of_lambdas = WalkGeneratorBackup.R * SegmentGeneratorBackup.LAMBDA;

    static Table<Vertex, Vertex, Double> C = HashBasedTable.create();
    public static void main(String[] args) throws IOException {
        ArrayList<VertexWalks> vertexWalks = VertexWalks.allVertexWalks;
        for (int i = 0; i < vertexWalks.size(); i++) {
            Vertex source = vertexWalks.get(i).sourceVertex;
            ArrayList<ArrayList<Vertex>> walks = vertexWalks.get(i).walks;
            for (int j = 0; j < walks.size(); j++) {
                ArrayList<Vertex> currentWalk = walks.get(j);
                for (int k = 0; k < currentWalk.size(); k++) {
                    if (C.get(source,currentWalk.get(k)) == null) {
                        C.put(source,currentWalk.get(k), 1.00);
                    }
                    else {
                        double oldValue = C.get(source,currentWalk.get(k));
                        C.remove(source,currentWalk.get(k));
                        C.put(source,currentWalk.get(k), oldValue+1.);
                    }
                }
            }
        }

        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("pi_hat.txt", true)));
        Set<Table.Cell<Vertex, Vertex, Double>> s = C.cellSet();
        Iterator<Table.Cell<Vertex, Vertex, Double>> it = s.iterator();
        while (it.hasNext()) {
            Table.Cell<Vertex, Vertex, Double> currentCell = it.next();
            writer.println("u = " + currentCell.getRowKey().getVertexID() + ", v = " + currentCell.getColumnKey().getVertexID() + ": " + currentCell.getValue()/sum_of_lambdas);
        }
        writer.println("------------------------------------------------------------------------");
        writer.close();

    }

}