package com.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class hdfsClientDemo {
    FileSystem fs = null;
    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://centos01:9000");
        //拿文件系统
        fs = FileSystem.get(new URI("hdfs://192.168.23.134:9000"), conf, "hadoop");

    }

    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(new Path("H:\\java\\传智云计算三期\\杂记.txt"),new Path("/杂记.txt.copy"));
        fs.close();
    }


}
