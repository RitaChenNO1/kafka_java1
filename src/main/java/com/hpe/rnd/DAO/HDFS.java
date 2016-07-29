package com.hpe.rnd.DAO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import com.hpe.rnd.DAO.Log;

/**
 * Created by zengliu on 7/28/2016.
 */
public class HDFS {

    private Properties HDFSProperties;
    private String host;
    private String hdfs_dir;
    private String hdfs_name;

    public HDFS() {

    }

    public HDFS(String propertyFile) throws IOException, SQLException {
        Properties HDFSProperties = new Properties();
        HDFSProperties.load(ClassLoader.getSystemResourceAsStream(propertyFile));
        host = HDFSProperties.getProperty("host");
        hdfs_dir = HDFSProperties.getProperty("hdfs_dir");
        hdfs_name = HDFSProperties.getProperty("hdfs_name");


    }

    public void writeHDFS(String source, String filename) throws IOException {
        Configuration conf = new Configuration();
        conf.set(hdfs_name, host);
        URI uri = URI.create(hdfs_dir);
        FileSystem fileSystem = FileSystem.get(uri, conf);
        System.out.println("connect hdfs successfully");
        Path HDFS_path = new Path(hdfs_dir + "/" + filename);
        System.out.println("connect hdfs successfully");
        // check if the file already exist
        if (fileSystem.exists(HDFS_path)) {
            System.out.println("File" + HDFS_path + "already exist, so append message to the file");
            FSDataOutputStream fileoutStream = fileSystem.append(HDFS_path);
            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fileoutStream));
            br.append("\n" + source);
            br.close();
            fileoutStream.close();
        } else {
            FSDataOutputStream out = fileSystem.create(HDFS_path);
            out.write(source.getBytes());
            out.close();
            System.out.println("new file \t" + conf.get("fs.default.name") + HDFS_path);
            fileSystem.close();
        }
    }


    public Properties getHDFSProperties() {
        return HDFSProperties;
    }

    public void setHDFSProperties(Properties HDFSProperties) {
        this.HDFSProperties = HDFSProperties;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String get_dir() {
        return hdfs_dir;
    }

    public void set_dir(String hdfs_dir) {
        this.hdfs_dir = hdfs_dir;
    }


    public String gethdfs_defname() {
        return hdfs_name;
    }

    public void sethdfs_defname(String hdfs_name) {
        this.hdfs_name = hdfs_name;
    }

}
