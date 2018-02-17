import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

/**
 * Created by filipgulan on 25/03/2017.
 */
public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {
        BasicConfigurator.configure(new NullAppender());

        Configuration configuration = new Configuration();
        LocalFileSystem lfs = LocalFileSystem.getLocal(configuration);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

        Path hdfsPath = new Path("/user/rovkp");
        Path lfsPath = new Path("/Users/filipgulan/ROVKP_DZ1/gutenberg.zip");


        lfs.close();
        hdfs.close();
    }
}
