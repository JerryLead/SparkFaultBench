package mllib;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.IOException;

/**
 * Created by wjf on 2016/8/19.
 */
public class HdfsHelp {

    public static void main(String[] args) throws IOException {
//        uploadFile2HDFS("dataGenerated/mllib/wjf/Kmeans_random.txt","hdfs://133.133.134.108:9000/user/hadoop/data/wjf");
    }

    public static void uploadFile2HDFS(String src,String dest) throws IOException {
        Configuration conf =new Configuration();
        conf.set("fs.defaultFS","hdfs://133.133.134.108:9000");
        FileSystem fs = FileSystem.get(conf);
        Path destpath = new Path(dest);
        Path srcpath =new Path(src);
        fs.copyFromLocalFile(false,srcpath,destpath);
        System.out.println("successed");
        FileStatus[] fileStatus=fs.listStatus(destpath);
        for( FileStatus file: fileStatus){
            System.out.println(file.getPath());
        }
        fs.close();

    }

}
