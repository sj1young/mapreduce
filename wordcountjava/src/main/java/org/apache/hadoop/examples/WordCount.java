package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;

public class WordCount {

    public static class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
        }
    }
}


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
    private TreeMap<Integer, Text> tmap;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException
    {
        tmap = new TreeMap<Integer, Text>();
    }

    public void reduce(Text key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
        sum += val.get();
        }
        tmap.put(new Integer(sum), key);

        // we remove the first key-value
        // if it's size increases 10
        if (tmap.size() > 10)
        {
            tmap.remove(tmap.lastKey());
        }

    }
    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException
    {

        for (Map.Entry<Integer, Text> entry : tmap.entrySet())
        {

            int count = entry.getKey();
            //String name = entry.getValue();
            context.write(new Text("nice"), new IntWritable(69));
        }
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: wordcount <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}