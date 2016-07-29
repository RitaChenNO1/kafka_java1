package com.hpe.rnd;

/**
 * Created by pengzhu on 7/21/2016.
 */

import com.hpe.rnd.DAO.HDFS;
import com.hpe.rnd.DAO.Log;
import com.hpe.rnd.DAO.Vertica;
import org.apache.hadoop.fs.*;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.Map.Entry;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.URL;

import static java.lang.System.exit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class test {

    public static void main(String[] args) throws IOException, SQLException {
        String str_packet = new String("this is a test for hdfs write!!!!");


        HDFS whdfs = new HDFS("HDFS.properties");
        whdfs.writeHDFS(str_packet,"test.txt");

//        String host = "hdfs://16.250.53.231:9000";
//        Log.Info(host);
//        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", host);
//        String hdfs = "hdfs://16.250.53.231:9000/test";
//        Path hdfs_path = new Path("hdfs://16.250.53.231:9000/test/test1.txt");
//        //here not just the host, it should be the path....
//        URI uri = URI.create(hdfs);
//        Log.Info("uri" + uri);
//        FileSystem fileSystem = FileSystem.get(uri, conf);
//        Log.Info(fileSystem.toString());
//        if (fileSystem.exists(hdfs_path)) {
//            FSDataOutputStream fileoutStream = fileSystem.append(hdfs_path);
//            BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fileoutStream));
//            br.append("\n" + str_packet);
//            br.close();
//            fileoutStream.close();
//        } else {
//            FSDataOutputStream outputStream = fileSystem.create(hdfs_path, true);
//            outputStream.writeUTF(str_packet);
//            outputStream.close();
//            System.out.println("new file \t" + conf.get("fs.default.name") + hdfs_path);
//            fileSystem.close();
//        }
//

    }


}

