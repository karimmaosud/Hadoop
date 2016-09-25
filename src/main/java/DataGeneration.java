import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * Created by karim on 21/09/16.
 */
public class DataGeneration {

    public static class AggregateMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{


        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "\t\t");
            Text newKey = new Text(itr.nextToken());
            Double price = Double.parseDouble(itr.nextToken());
            int quantity = Integer.parseInt(itr.nextToken());
            context.write(newKey, new DoubleWritable(price*quantity));
        }
    }

    public static class AggregateReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {


        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    static Product[] generateProducts(int numProducts, int minPrice, int maxPrice){
        Random rand = new Random();
        Product [] p = new Product[numProducts];
        int range = maxPrice-minPrice;
        for(int i=0; i<p.length; i++){
            p[i] = new Product(i, range * rand.nextDouble() + (double) minPrice);
        }
        return p;
    }

    public static void main(String [] args) throws Exception{

        Product[] products = generateProducts(30000, 5, 200);

        FileSystem fs = FileSystem.get(new Configuration());

        HDFSWriter writer = new HDFSWriter(fs, args[0]);
        Thread writerThread = new Thread(writer);
        writerThread.start();

        // schedule ten threads each generate one million user to the hdfs.

        int numUsers = 10000;
        for(int i =0; i<numUsers; i+=(numUsers/10)){
            new Thread(new Generator(i, numUsers/10, products, writer)).start();
        }

        writerThread.join();

        Configuration conf = new Configuration();
        Job aggregateJob = Job.getInstance(conf, "user transaction");
        aggregateJob.setJarByClass(DataGeneration.class);
        aggregateJob.setMapperClass(AggregateMapper.class);
        aggregateJob.setCombinerClass(AggregateReducer.class);
        aggregateJob.setReducerClass(AggregateReducer.class);
        aggregateJob.setOutputKeyClass(Text.class);
        aggregateJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(aggregateJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(aggregateJob, new Path(args[1]));

        System.exit(aggregateJob.waitForCompletion(true) ? 0 : 1);


        // verify the input :)
    }
}
