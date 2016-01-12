package ir.ac.ut.iis.ppr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.*;

import java.io.*;
import java.util.*;

public class SegmentGeneratorBackup {
    static int LAMBDA = Math.abs(StdRandom.geometric(0.15));
    static int THETA = 1;
    static int NO_SEGMENTS = (int) Math.ceil((double) LAMBDA / (double) THETA);
    static Map<SegmentGeneratorBackupVertex, StringWritable> SEGMENTS = new HashMap<SegmentGeneratorBackupVertex, StringWritable>();
    static Map<Text, SegmentGeneratorBackupVertex> MAP = new HashMap<Text, SegmentGeneratorBackupVertex>();


    public static class SegmentGeneratorBackupVertex extends Vertex<Text, NullWritable, StringWritable> {

        @Override
        public void compute(Iterable<StringWritable> stringWritables) throws IOException {
            LAMBDA = 70;
            THETA = 3;
            NO_SEGMENTS = (int) Math.ceil((double) LAMBDA / (double) THETA);
            System.out.println("LAMBDA: " + LAMBDA);
            genSeg();
        }

        public void genSeg() throws FileNotFoundException, UnsupportedEncodingException {
            if (this.getSuperstepCount() == 0) {

                if (! MAP.containsKey(this.getVertexID()))
                    MAP.put(this.getVertexID(), this);

                String value = new String();
                for (int i = 0; i < NO_SEGMENTS; i++) {
                    value = value + this.getVertexID();
                    if (i != NO_SEGMENTS - 1)
                        value = value + "-";
                }

                this.setValue(new StringWritable(value));
            }


            else if (this.getSuperstepCount() >= 1) {

                String oldValue = this.getValue().get();
                String[] segments = oldValue.split("-");

                for (int i = 1; i <= (int) Math.ceil((double) SegmentGeneratorBackup.LAMBDA / (double) SegmentGeneratorBackup.THETA); i++) {

                    System.out.println("a a a:"+(int) Math.ceil((double) SegmentGeneratorBackup.LAMBDA / (double) SegmentGeneratorBackup.THETA));
                    System.out.println("g g g:"+SegmentGeneratorBackup.NO_SEGMENTS);

                    if (  (i == (int) Math.ceil((double) SegmentGeneratorBackup.LAMBDA / (double) SegmentGeneratorBackup.THETA))
                            && ( (SegmentGeneratorBackup.LAMBDA - SegmentGeneratorBackup.THETA * Math.floor((double) SegmentGeneratorBackup.LAMBDA / (double) SegmentGeneratorBackup.THETA) ) > 0 )
                            && ( SegmentGeneratorBackup.LAMBDA - SegmentGeneratorBackup.THETA * Math.floor((double) SegmentGeneratorBackup.LAMBDA / (double) SegmentGeneratorBackup.THETA) ) < this.getSuperstepCount()  )
                        break;  // the last segment

                    String segment = segments[i-1];
                    String[] hops = segment.split(",");

                    Text lastHopID = new Text(hops[hops.length - 1]);
                    Vertex lastHop = MAP.get(lastHopID);

                    MapPrinter.print(SegmentGeneratorBackup.SEGMENTS, "segmentsmap.txt");

                    List<Edge<Text,NullWritable>> outedges = lastHop.getEdges();
                    Random generator = new Random();
                    int randomIndex = generator.nextInt(outedges.size() );   // divoone be khodesh outedge mide!
                    while (outedges.get(randomIndex).getDestinationVertexID() == this.getVertexID())
                        randomIndex = generator.nextInt(outedges.size() );
                    Edge randomEdge = outedges.get(randomIndex);
                    Text randomEdgeID = (Text) randomEdge.getDestinationVertexID();
                    segment = segment + ",";
                    segment = segment + randomEdgeID.toString();
                    segments[i-1] = segment;
                }

                String newValue = new String();
                for (int i = 0; i < segments.length; i++) {
                    newValue = newValue + segments[i];
                    if (i != segments.length - 1)
                        newValue = newValue + "-";
                }
                this.setValue(new StringWritable(newValue));


                if (this.getSuperstepCount() == this.getMaxIteration()) {
                    SEGMENTS.put(this, this.getValue());

                    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("segmentsaaaaaaaaaaaaaa.txt", true)))) {
                        out.println("*** lambda = " + LAMBDA + " ***    " + this.toString() + " |    " + this.getValue().get());
                        out.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
        }

        public String toString() {
            return this.getVertexID().toString();
        }
    }

    public static class SegmentGeneratorBackupSeqReader extends VertexInputReader<Text, TextArrayWritable, Text, NullWritable, StringWritable> {
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
        GraphJob segmentGeneratorBackupJob = new GraphJob(conf, SegmentGeneratorBackup.class);
        segmentGeneratorBackupJob.setJobName("SegmentGeneratorBackup");
        segmentGeneratorBackupJob.setVertexClass(SegmentGeneratorBackupVertex.class);
        segmentGeneratorBackupJob.setInputPath(new Path(args[0]));
        segmentGeneratorBackupJob.setOutputPath(new Path(args[1]));
        segmentGeneratorBackupJob.setMaxIteration(3);
        segmentGeneratorBackupJob.set("hama.graph.self.ref", "true");
        segmentGeneratorBackupJob.set("hama.graph.max.convergence.error", "0.001");
        segmentGeneratorBackupJob.setVertexInputReaderClass(SegmentGeneratorBackupSeqReader.class);
        segmentGeneratorBackupJob.setVertexIDClass(Text.class);
        segmentGeneratorBackupJob.setVertexValueClass(StringWritable.class);
        segmentGeneratorBackupJob.setEdgeValueClass(NullWritable.class);
        segmentGeneratorBackupJob.setInputFormat(SequenceFileInputFormat.class);
        segmentGeneratorBackupJob.setPartitioner(HashPartitioner.class);
        segmentGeneratorBackupJob.setOutputFormat(TextOutputFormat.class);
        segmentGeneratorBackupJob.setOutputKeyClass(Text.class);
        segmentGeneratorBackupJob.setOutputValueClass(StringWritable.class);

        return segmentGeneratorBackupJob;
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {


        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob segmentGeneratorBackupJob = createJob(args, conf);
        long startTime = System.currentTimeMillis();
        if (segmentGeneratorBackupJob.waitForCompletion(true))
            System.out.println("segmentGeneratorBackupJob Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds.");
    }

    public static void runMyJob(String[] args, Text vertexID, long superstepCount) throws IOException, ClassNotFoundException, InterruptedException {
        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob segmentGeneratorBackupJob = createJob(args, conf);
        SegmentGeneratorBackupVertex sgv = new SegmentGeneratorBackupVertex();
        Iterable<StringWritable> stringWritables = null;
        long startTime = System.currentTimeMillis();
        sgv.compute(stringWritables);
        System.out.println("Vertex "+vertexID+", iteration "+superstepCount+": segmentGeneratorBackupJob Finished in "
                + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds.");
    }

}