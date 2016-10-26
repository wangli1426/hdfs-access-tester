package edu.illinois.adsc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.net.URI;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        testAppend();
    }

    static void testRead() {
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        Configuration configuration = new Configuration();
        try {

            URI uri = URI.create("hdfs://192.168.0.237:54310/robert/test");
            distributedFileSystem.initialize(uri, configuration);
            Path path = new Path("hdfs://192.168.0.237:54310/robert/test");
            FSDataInputStream fsDataInputStream = distributedFileSystem.open(path);
            final int readBytes = 1024 * 1024 * 256;
            final int step = Math.max(1024 * 1024 * 64, readBytes);
            byte[] bytes = new byte[readBytes];
            final int runs = 10;
            long totalTime = 0;
            for(int i = 0; i < runs; i++) {
                final long start = System.currentTimeMillis();
                fsDataInputStream.readFully(i * step, bytes, 0, readBytes);
                final long time = System.currentTimeMillis() - start;
                totalTime += time;
                System.out.println(String.format("%d ms.", time));
            }
            System.out.println(String.format("Avg: %d ms", totalTime / runs));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void testAppend() {
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        Configuration configuration = new Configuration();
        try {

            URI uri = URI.create("hdfs://192.168.0.237:54310/robert/test");
            distributedFileSystem.initialize(uri, configuration);
            Path path = new Path("hdfs://192.168.0.237:54310/robert/test");
            byte[] bytes = new byte[1024 * 1024 * 64];

            final int runs = 10;
            long totalTime = 0;
            FSDataOutputStream fsDataOutputStream = null;
            for(int i = 0; i < runs; i++) {
                final long start = System.currentTimeMillis();
                if(fsDataOutputStream == null)
                    fsDataOutputStream = distributedFileSystem.append(path);
                fsDataOutputStream.write(bytes);
                fsDataOutputStream.flush();
//                fsDataOutputStream.close();
                final long time = System.currentTimeMillis() - start;
                totalTime += time;
                System.out.println(String.format("%d ms.", time));
            }
            fsDataOutputStream.close();

            System.out.println("Avg time: " + totalTime / runs);
//            fsDataInputStream.close();
            distributedFileSystem.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
