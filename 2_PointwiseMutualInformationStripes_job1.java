// package io.bespin.java.mapreduce.cooccur;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

// import tl.lin.data.map.HMapStIW;

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
public class PointwiseMutualInformationStripes_job1 extends Configured
		implements Tool {
	private static final Logger LOG = Logger
			.getLogger(PointwiseMutualInformationStripes_job1.class);

	private static class MyMapper extends
			Mapper<LongWritable, Text, Text, HMapStIW> {
		private static final HMapStIW MAP = new HMapStIW();
		private static final Text KEY = new Text();

		private int window = 2;

		@Override
		public void setup(Context context) {
			window = context.getConfiguration().getInt("window", 2);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = ((Text) value).toString();
			final int LIMIT = 100;
			int count = 0;

			List<String> tokens = new ArrayList<String>();
			StringTokenizer itr = new StringTokenizer(line);

			MAP.clear();
			while (itr.hasMoreTokens()) {
				String w = itr.nextToken().toLowerCase()
						.replaceAll("(^[^a-z]+|[^a-z]+$)", "");
				if (w.length() == 0)
					continue;
				tokens.add(w);

				// we are only going to consider up to the first 100 words in
				// each line.
				if (count > LIMIT)
					break;

				// count words
				MAP.increment("*");
			}// while()

			// emit word count
			context.write(new Text("*"), MAP);

			for (int i = 0; i < tokens.size(); i++) {
				KEY.set(tokens.get(i));
				MAP.clear();
				MAP.increment(tokens.get(i));
				
				for (int j = Math.max(i - window, 0); j < Math.min(i + window
						+ 1, tokens.size()); j++) {
					if (i == j)
						continue;
					
					// avoid duplicate tokens
					if (tokens.get(i).equals(tokens.get(j)))
						continue;

					MAP.increment(tokens.get(j));
				}// for()

				context.write(KEY, MAP);
			}// for()
		}// map()
	}// MyMapper()

	private static class MyReducer extends
			Reducer<Text, HMapStIW, Text, HMapStIW> {
		private int wordcount = 0;
		private int marginal = 0;
		
		@Override
		public void reduce(Text key, Iterable<HMapStIW> values, Context context)
				throws IOException, InterruptedException {
			Iterator<HMapStIW> iter = values.iterator();
			HMapStIW map = new HMapStIW();
			HMapStIW actual = new HMapStIW();
			final int LIMIT = 10;

			while (iter.hasNext())
				map.plus(iter.next());

			for (MapKI.Entry<String> e : map.entrySet()) {
				if (e.getKey().equals(key.toString()))
					marginal = e.getValue();
				else if (e.getValue() >= LIMIT)
					actual.put(e.getKey(), e.getValue());				
			}// for()
			
			if (key.toString().equals("*"))
			{
				// total word count
				for (MapKI.Entry<String> e : map.entrySet())
					wordcount += e.getValue();
			}
			else
			{
				key.set(key.toString() + " " + Integer.toString(wordcount)
						 + " " + Integer.toString(marginal));
				context.write(key, actual);
			}
		}// reduce()
	}// MyReducer()

	/**
	 * Creates an instance of this tool.
	 */
	public PointwiseMutualInformationStripes_job1() {
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
		 * CmdLineParser(args, ParserProperties
		 * .defaults().withUsageWidth(100));
		 * 
		 * try { parser.parseArgument(argv); } catch (CmdLineException e) {
		 * System.err.println(e.getMessage()); parser.printUsage(System.err);
		 * return -1; }
		 * 
		 * LOG.info("Tool: " +
		 * ComputeCooccurrenceMatrixPairs.class.getSimpleName());
		 * LOG.info(" - input path: " + args.input); LOG.info(" - output path: "
		 * + args.output); LOG.info(" - window: " + args.window);
		 * LOG.info(" - number of reducers: " + args.numReducers);
		 */
		Job job = Job.getInstance(getConf());
		job.setJobName(PointwiseMutualInformationStripes_job1.class
				.getSimpleName());
		job.setJarByClass(PointwiseMutualInformationStripes_job1.class);

		// Delete the output directory if it exists already.
		Path outputDir = new Path(argv[1]);
		FileSystem.get(getConf()).delete(outputDir, true);

		// job.getConfiguration().setInt("window", args.window);

		// job.setNumReduceTasks(args.numReducers);
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, new Path(argv[0]));
		FileOutputFormat.setOutputPath(job, new Path(argv[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(HMapStIW.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HMapStIW.class);

		job.setMapperClass(MyMapper.class);
		// job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);

		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		System.out.println("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		return 0;
	}// run()

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PointwiseMutualInformationStripes_job1(), args);
	}
}// PointwiseMutualInformationStripes_job1()
