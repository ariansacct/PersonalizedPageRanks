package ir.ac.ut.iis.ppr;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.graph.Vertex;

import java.io.IOException;
import java.util.ArrayList;

public class VertexWalks {

    static ArrayList<VertexWalks> allVertexWalks = new ArrayList<VertexWalks>();
    Vertex sourceVertex;
    ArrayList<ArrayList<Vertex>> walks;


    VertexWalks(Vertex v) {
        sourceVertex = v;
        walks = new ArrayList<ArrayList<Vertex>>();
    }

    public static VertexWalks vertexWalksFinder(Vertex v) {
        int i;
        for (i = 0; i < allVertexWalks.size(); i++) {
            if (allVertexWalks.get(i).sourceVertex.getVertexID().equals(v.getVertexID()))
                break;
        }
        return allVertexWalks.get(i);
    }
}
