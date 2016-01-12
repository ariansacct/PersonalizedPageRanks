package ir.ac.ut.iis.ppr;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MapPrinter {

    public static void print(Map<SegmentGeneratorBackup.SegmentGeneratorBackupVertex, StringWritable> m, String filename) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter printWriter = new PrintWriter(filename, "UTF-8");
        Set ks = m.keySet();
        Iterator it = ks.iterator();
        while (it.hasNext()) {
            Object current = it.next();
            printWriter.println(current + ": " + m.get(current).get());
        }

        printWriter.println("---------------------------------------------------------------------");
        printWriter.close();
    }
}
