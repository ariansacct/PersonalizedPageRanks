package ir.ac.ut.iis.ppr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WalkGeneratorBackup {

    static Map<Text, StringWritable> walks = new HashMap<Text, StringWritable>();
    static int R = 11;

    public static class WalkGeneratorBackupVertex extends Vertex<Text, NullWritable, StringWritable> {

        ArrayList<Vertex> al;

        public void compute(Iterable<StringWritable> stringWritables) throws IOException {

            if (this.getSuperstepCount() == 0) {

                MapPrinter.print(SegmentGeneratorBackup.SEGMENTS, "beforeshoorooe.txt");

                String value = new String();
                value = value + this.getVertexID() + ",";     // zaheran injoori too cast-e Text be String moshkeli nadare
                this.setValue(new StringWritable(value));
                VertexWalks.allVertexWalks.add(new VertexWalks(this));
            }

            else {
                al = new ArrayList<Vertex>();
                String value = new String();
                value = value + this.getVertexID() + ",";     // zaheran injoori too cast-e Text be String moshkeli nadare
                this.setValue(new StringWritable(value));

                for(int i = 1; i <= SegmentGeneratorBackup.NO_SEGMENTS; i++) {
                    String oldValueString = this.getValue().get();
                    String[] segments = oldValueString.split(",");
                    String lastNodeString = segments[segments.length - 1];
                    Text lastNodeText = new Text(lastNodeString);
                    SegmentGeneratorBackup.SegmentGeneratorBackupVertex lastNodeVertex = SegmentGeneratorBackup.MAP.get(lastNodeText);
                    StringWritable lastNodeValue = SegmentGeneratorBackup.SEGMENTS.get(lastNodeVertex);
                    String[] lastNodeSegments = lastNodeValue.get().split("-");
                    String ithSegment = new String(lastNodeSegments[i - 1]);
                    int commaIndex = ithSegment.indexOf(',');
                    String toBeAppended = new String(lastNodeSegments[i - 1].substring(commaIndex + 1));

                    String[] tba = toBeAppended.split(",");
                    for (int ind = 0; ind < tba.length; ind++) {
                        al.add(SegmentGeneratorBackup.MAP.get(new Text(tba[ind])));
                    }

                    String newValueString = oldValueString + toBeAppended;
                    if (i != SegmentGeneratorBackup.NO_SEGMENTS)
                        newValueString = newValueString + ",";
                    this.setValue(new StringWritable(newValueString));
                }
                try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("newnewwalks.txt", true)))) {
                    out.println("walk no " + this.getSuperstepCount() + " for Vertex " + this.getVertexID().toString() + ": " + this.getValue().get());
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }



                walks.put(this.getVertexID(), this.getValue());
                VertexWalks.vertexWalksFinder(this).walks.add(al);
            }
        }
    }

    public static class WalkGeneratorBackupSeqReader extends VertexInputReader<Text, TextArrayWritable, Text, NullWritable, StringWritable> {
        @Override
        public boolean parseVertex(Text key, TextArrayWritable value, Vertex<Text, NullWritable, StringWritable> vertex) throws Exception {
            vertex.setVertexID(key);

            for (Writable v : value.get()) {
                vertex.addEdge(new Edge<Text, NullWritable>((Text) v, null));
            }

            return true;
        }
    }

    public static GraphJob createJob(String[] args, HamaConfiguration conf)
            throws IOException {
        GraphJob walkGeneratorBackupJob = new GraphJob(conf, WalkGeneratorBackup.class);
        walkGeneratorBackupJob.setJobName("WalkGeneratorBackup");
        walkGeneratorBackupJob.setVertexClass(WalkGeneratorBackupVertex.class);
        walkGeneratorBackupJob.setInputPath(new Path(args[0]));
        walkGeneratorBackupJob.setOutputPath(new Path(args[1]));
        walkGeneratorBackupJob.setMaxIteration(R);
        walkGeneratorBackupJob.set("hama.graph.self.ref", "true");
        walkGeneratorBackupJob.set("hama.graph.max.convergence.error", "0.001");
        walkGeneratorBackupJob.setVertexInputReaderClass(WalkGeneratorBackupSeqReader.class);
        walkGeneratorBackupJob.setVertexIDClass(Text.class);
        walkGeneratorBackupJob.setVertexValueClass(StringWritable.class);
        walkGeneratorBackupJob.setEdgeValueClass(NullWritable.class);
        walkGeneratorBackupJob.setInputFormat(SequenceFileInputFormat.class);
        walkGeneratorBackupJob.setPartitioner(HashPartitioner.class);
        walkGeneratorBackupJob.setOutputFormat(TextOutputFormat.class);
        walkGeneratorBackupJob.setOutputKeyClass(Text.class);
        walkGeneratorBackupJob.setOutputValueClass(StringWritable.class);

        return walkGeneratorBackupJob;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        SegmentGeneratorBackup.main(args);
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob walkGeneratorBackupJob = createJob(args, conf);
        long startTime = System.currentTimeMillis();
        walkGeneratorBackupJob.waitForCompletion(true);
        System.out.println("walkGeneratorBackupJob Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds.");

        Analysis.main(args);
    }

}