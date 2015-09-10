import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job job1 = Job.getInstance(conf, "Link Count");
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapperClass(LinkCountMap.class);
        job1.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tmpPath);

        job1.setJarByClass(PopularityLeague.class);
        if(!job1.waitForCompletion(true)) {
            return 0;
        }

        Job job2 = Job.getInstance(conf, "Top Links");
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(IntArrayWritable.class);
        job2.setMapperClass(PopularityLeagueMapper.class);
        job2.setReducerClass(PopularityLeagueReducer.class);

        FileInputFormat.setInputPaths(job2, tmpPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setJarByClass(PopularityLeague.class);
        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer recordTokenizer = new StringTokenizer(value.toString(), ":");
            Integer nodeId = Integer.parseInt(recordTokenizer.nextToken().trim());
            StringTokenizer linkListTokenizer = new StringTokenizer(recordTokenizer.nextToken(), " ");
            while (linkListTokenizer.hasMoreTokens()) {
                Integer linkId = Integer.parseInt(linkListTokenizer.nextToken());
                context.write(new IntWritable(linkId), new IntWritable(nodeId));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private Set<Integer> leagueIdSet = new HashSet<>();

        @Override
        protected void setup(Reducer.Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            for (String leagueIdString: Arrays.asList(readHDFSFile(conf.get("league"), conf).split("\n"))) {
                this.leagueIdSet.add(Integer.parseInt(leagueIdString));
            }
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if (this.leagueIdSet.contains(key.get())) {
                Integer linkCount = IteratorUtils.toList(values.iterator()).size();
                context.write(key, new IntWritable(linkCount));
            }
        }
    }

    public static class PopularityLeagueMapper extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer nodeId = Integer.parseInt(key.toString());
            Integer count = Integer.parseInt(value.toString());
            Integer[] contNodeIdArray = {nodeId, count};
            context.write(NullWritable.get(), new IntArrayWritable(contNodeIdArray));
        }

    }

    public static class PopularityLeagueReducer extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> countList = this.makeCountList(values);
            for (IntArrayWritable nodeIdCountIntArray: values) {
                List<IntWritable> nodeIdCount = Arrays.asList((IntWritable[]) nodeIdCountIntArray.toArray());
                Integer rank = countList.indexOf(nodeIdCount.get(1).get());
                context.write(new IntWritable(nodeIdCount.get(0).get()), new IntWritable(rank));
            }
        }

        private List<Integer> makeCountList(Iterable<IntArrayWritable> values) {
            Set<Integer> retSet = new HashSet<>();
            for (IntArrayWritable nodeIdCountIntArray: values) {
                List<IntWritable> nodeIdCount = Arrays.asList((IntWritable[]) nodeIdCountIntArray.toArray());
                retSet.add(nodeIdCount.get(1).get());
            }
            List<Integer> ret = new ArrayList<>(retSet);
            Collections.sort(ret, Collections.reverseOrder());
            return ret;
        }
    }

    private static String readHDFSFile(String path, Configuration conf) throws IOException {
        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while ((line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    private static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

}
