import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

import java.io.IOException;
import java.lang.Integer;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

// >>> Don't Change
public class TopPopularLinks extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
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
// <<< Don't Change

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

        job1.setJarByClass(TopPopularLinks.class);
        if(!job1.waitForCompletion(true)) {
            return 0;
        }

        Job job2 = Job.getInstance(conf, "Top Links");
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(IntArrayWritable.class);
        job2.setMapperClass(TopLinksMap.class);
        job2.setReducerClass(TopLinksReduce.class);

        FileInputFormat.setInputPaths(job2, tmpPath);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setJarByClass(TopPopularLinks.class);
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
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer linkCount = IteratorUtils.toList(values.iterator()).size();
            context.write(key, new IntWritable(linkCount));
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private Integer N;
        private TreeSet<Pair<Integer, Integer>> countNodeIdMap = new TreeSet<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer nodeId = Integer.parseInt(key.toString());
            Integer count = Integer.parseInt(value.toString());
            this.countNodeIdMap.add(new Pair<>(count, nodeId));
            if (this.countNodeIdMap.size() > this.N) {
                this.countNodeIdMap.remove(this.countNodeIdMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> countNodeId: this.countNodeIdMap) {
                Integer[] contNodeIdArray = {countNodeId.second, countNodeId.first};
                context.write(NullWritable.get(), new IntArrayWritable(contNodeIdArray));
            }
        }

    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        private Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            TreeSet<Pair<Integer, Integer>> countNodeMap = this.makeCountNodeMap(values);
            for (Pair<Integer, Integer> countNode: countNodeMap) {
                context.write(new IntWritable(countNode.second), new IntWritable(countNode.first));
            }
        }

        private TreeSet<Pair<Integer, Integer>> makeCountNodeMap(Iterable<IntArrayWritable> values) {
            TreeSet<Pair<Integer, Integer>> countNodeMap = new TreeSet<>();
            for (IntArrayWritable nodeCountTextArray: values) {
                List<Integer> nodeCount = Arrays.asList((Integer[]) nodeCountTextArray.toArray());
                countNodeMap.add(new Pair<>(nodeCount.get(1), nodeCount.get(0)));
                if (countNodeMap.size() > this.N) {
                    countNodeMap.remove(countNodeMap.first());
                }
            }
            return countNodeMap;
        }

    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change
