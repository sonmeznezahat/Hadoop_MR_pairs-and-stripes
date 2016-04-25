	// package io.bespin.java.mapreduce.cooccur;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

// import tl.lin.data.pair.PairOfStrings;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices
 * from a large text collection. This algorithm is described in Chapter 3 of
 * "Data-Intensive Text Processing with MapReduce" by Lin &amp; Dyer, as well as
 * the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the
 * Masses: A Case Study in Computing Word Co-occurrence Matrices with
 * MapReduce.</b> <i>Proceedings of the 2008 Conference on Empirical Methods in
 * Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class pairs_pmi extends Configured implements Tool
{
	private static final Logger LOG = Logger
			.getLogger(pairs_pmi.class);

	private static class MyMapper extends
			Mapper<LongWritable, Text, PairOfStrings, IntWritable>
	{
		private static final PairOfStrings PAIR = new PairOfStrings();
		private static final IntWritable VALUE = new IntWritable(1);
		private int window = 2;

		@Override
		public void setup(Context context)
		{
			window = context.getConfiguration().getInt("window", 2);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			String line = ((Text) value).toString();

			StringTokenizer itr = new StringTokenizer(line);
			String x;
			String y;
			String wordcount;
			String marginal;
			String joint;
			
			if (!itr.hasMoreTokens())
				return;
			
			x = itr.nextToken();
			y = itr.nextToken();
			wordcount = itr.nextToken();
			marginal = itr.nextToken();
			joint = itr.nextToken();

			PAIR.set(y, x + " " + wordcount + " " + marginal + " " + joint);
			context.write(PAIR, VALUE);
			
			PAIR.set(y, "*");
			VALUE.set(Integer.parseInt(marginal));
			context.write(PAIR, VALUE);
		}
	}

	private static class MyReducer extends
			Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable>
	{
		private static final PairOfStrings PAIR = new PairOfStrings();
		private final static DoubleWritable VALUE = new DoubleWritable();
		private int others = 0;
		
		@Override
		public void reduce(PairOfStrings key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException
		{
			Iterator<IntWritable> iter = values.iterator();
			int sum = 0;
			double result = 0.0f;
			
			while (iter.hasNext()) {
				sum += iter.next().get();
			}
			
			if (key.getRightElement().equals("*"))
			{
				others = sum;
			}
			else
			{
				StringTokenizer itr = new StringTokenizer(key.getRightElement());
				String x;
				String y;
				String wordcount;
				String marginal;
				String joint;
				double ny;
				
				y = key.getLeftElement();
				x = itr.nextToken();
				wordcount = itr.nextToken();
				marginal = itr.nextToken();
				joint = itr.nextToken();

				double nx = Integer.parseInt(marginal);
				double nxy = Integer.parseInt(joint);
				double N = Integer.parseInt(wordcount);
				
				if(N - others > 0)
				{
				 ny = N - others; }
				else 
					 ny = 1 ;


				result = Math.log10((nxy * N) / (nx * ny));
				// result = N * 1000000 + nx * 10000 + ny * 100 + nxy;
	
				PAIR.set(x, y);
				VALUE.set(result);
				context.write(PAIR, VALUE);
			}
		}
	}

	protected static class MyPartitioner extends
			Partitioner<PairOfStrings, IntWritable>
	{
		@Override
		public int getPartition(PairOfStrings key, IntWritable value,
				int numReduceTasks)
		{
			return (key.getLeftElement().hashCode() & Integer.MAX_VALUE)
					% numReduceTasks;
		}
	}

	/**
	 * Creates an instance of this tool.
	 */
	public pairs_pmi() {
	}

	public static class Args {
		@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
		public String input;

		@Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
		public String output;

		@Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
		public int numReducers = 1;

		@Option(name = "-window", metaVar = "[num]", required = false, usage = "cooccurrence window")
		public int window = 2;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] argv) throws Exception {
		/*
		 * Args args = new Args(); CmdLineParser parser = new
		 * CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
		 * 
		 * try { parser.parseArgument(argv); } catch (CmdLineException e) {
		 * System.err.println(e.getMessage()); parser.printUsage(System.err);
		 * return -1; }
		 * 
		 * LOG.info("Tool: " +
		 * PointwiseMutualInformation_job1.class.getSimpleName());
		 * LOG.info(" - input path: " + args.input); LOG.info(" - output path: "
		 * + args.output); LOG.info(" - window: " + args.window);
		 * LOG.info(" - number of reducers: " + args.numReducers);
		 */
		Job job = Job.getInstance(getConf());
		job.setJobName(pairs_pmi.class.getSimpleName());
		job.setJarByClass(pairs_pmi.class);

		// Delete the output directory if it exists already.
		Path outputDir = new Path(argv[1]);
		FileSystem.get(getConf()).delete(outputDir, true);

		job.getConfiguration().setInt("window", 100);

		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapOutputKeyClass(PairOfStrings.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(PairOfStrings.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new pairs_pmi(), args);
	}
}
