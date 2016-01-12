package ir.ac.ut.iis.ppr;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TextArrayWritable;
import org.apache.hama.examples.util.SymmetricMatrixGen;
import org.junit.Test;

import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Created with IntelliJ IDEA.
 * User: arian
 * Date: 10/16/13
 * Time: 10:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class SymmetricMatrixGenTest {
    protected static Log LOG = LogFactory.getLog(SymmetricMatrixGenTest.class);
    private static String TEST_OUTPUT = "/home/arian/IdeaProjects/PPR/fullinput";

    @Test
    public void testGraphGenerator() throws Exception {
        try(PrintWriter printWriter = new PrintWriter("/home/arian/IdeaProjects/PPR/graph.txt", "UTF-8")) {

            Configuration conf = new Configuration();

            SymmetricMatrixGen.main(new String[] { "3", "1", TEST_OUTPUT, "1" });
            FileSystem fs = FileSystem.get(conf);

            FileStatus[] globStatus = fs.globStatus(new Path(TEST_OUTPUT + "/part-*"));
            for (FileStatus fts : globStatus) {
                try(SequenceFile.Reader reader = new SequenceFile.Reader(fs, fts.getPath(),
                        conf)) {
                    Text key = new Text();
                    TextArrayWritable value = new TextArrayWritable();

                    while (reader.next(key, value)) {
                        String values = "";
                        for (Writable v : value.get()) {
                            values += v.toString() + " ";
                        }
                        LOG.info(fts.getPath() + ": " + key.toString() + " | " + values);

                        printWriter.write(key.toString() + " | " + values);
                        System.out.println("key ine: "+key.toString());
                        printWriter.write('\n');
                    }
                }
            }

        }
    }
}
