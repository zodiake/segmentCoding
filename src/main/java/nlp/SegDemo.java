package nlp;

import java.util.Properties;

/**
 * Created by wangqi08 on 11/8/2016.
 */
public class SegDemo {

    private static final String basedir = System.getProperty("SegDemo", "data");

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.setProperty("sighanCorporaDict", basedir);
        // props.setProperty("NormalizationTable", "data/norm.simp.utf8");
        // props.setProperty("normTableEncoding", "UTF-8");
        // below is needed because CTBSegDocumentIteratorFactory accesses it
        props.setProperty("serDictionary", basedir + "/dict-chris6.ser.gz");
        props.setProperty("inputEncoding", "UTF-8");
        props.setProperty("sighanPostProcessing", "true");


    }
}
