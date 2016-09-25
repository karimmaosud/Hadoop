import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Created by karim on 21/09/16.
 */
public class HDFSWriter implements Runnable{

    private ConcurrentLinkedQueue<String> lines;
    private FileSystem fs;
    private BufferedWriter bw;

    HDFSWriter(FileSystem fs, String path){
        lines = new ConcurrentLinkedQueue<String>();
        this.fs = fs;
        try{
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path+"/input.txt"))));

        }catch(Exception e){e.printStackTrace();}
    }

    @Override
    public void run() {

        int finishedThreads = 0;
        while(finishedThreads < 10){
            if(lines.size() > 0){
                String line = lines.poll();
                if(line.compareTo("-1") == 0){
                    finishedThreads ++;
                }else{
                    try{
                        bw.write(line+"\n");
                    }catch (Exception e){e.printStackTrace();}
                }
            }else{
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) { e. printStackTrace();}
            }
        }
        try{
            bw.close();
        }catch(Exception e){}
    }
    public void addLineToQueue(String line){
        this.lines.add(line);
    }
}
