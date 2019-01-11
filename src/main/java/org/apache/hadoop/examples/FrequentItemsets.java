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

public class FrequentItemsets {

  public static int SUPPORT = 100;  // The support for frequent itemsets
  public static int HASH_LEN = 100000;  // The length of the hash table

  /* Mapper for the pass 1 of PCY algorithm, to find the frequent pairs */
  /* Input: <list of items> */
  /* Output: "Basket" <basket items> */
  /*         <item> "1" */
  /*         <hash value> "1" */
  public static class PCYPass1Mapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

      // Read each basket
      while (itr.hasMoreTokens()) {
        String basketStr = itr.nextToken();

        // Output the basket items
        keyText.set("Basket");
        valueText.set(basketStr);
        context.write(keyText, valueText);

        String[] items = basketStr.split(",");
        for (int i = 0; i < items.length; i++) {

          // Read each item, and add 1 for each item's count
          String item = items[i];
          keyText.set(item);
          valueText.set("1");
          context.write(keyText, valueText);

          // Hash each pair of items
          int i1 = ((item.hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
          for (int j = i + 1; j < items.length; j++) {
            int i2 = ((items[j].hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
            int hashValue = (((i1 + i2) % HASH_LEN) + HASH_LEN) % HASH_LEN;
            keyText.set(String.valueOf(hashValue));
            valueText.set("1");
            context.write(keyText, valueText);
          }
        }
      }
    }
  }

  /* Reducer for the pass 1 of PCY algorithm, to find the frequent pairs */
  /* Input: "Basket" <basket items> */
  /*        <item> "1" */
  /*        <hash value> "1" */
  /* Output: "Basket" <basket items> */
  /*         "Freq item" <(item, count)> */
  /*         "Hash" <(hash value, count)> */
  public static class PCYPass1Reducer extends Reducer<Text, Text, Text, Text> {
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String keyStr = key.toString();

      if (keyStr.indexOf('B') != -1) {

        // Output the basket items
        for (Text val : values) {
          context.write(key, val);
        }
      }
      else {
        
        // Count each bucket or item
        int sum = 0;
        for (Text val : values) {
          sum += 1;
        }

        // Only output the buckets or items that pass the support
        if (sum >= SUPPORT) {
          if (keyStr.matches("-?\\d+")) {
            keyText.set("Hash");
          }
          else {
            keyText.set("Freq item");
          }
          valueText.set("(" + keyStr + ", " + String.valueOf(sum) + ")");
          context.write(keyText, valueText);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: frequentitemsets <in> <out>");
      System.exit(2);
    }

    // PCY algorithm Pass 1: Count the items, and hash each pair
    Job job1 = new Job(conf, "PCY Pass 1");
    job1.setJarByClass(FrequentItemsets.class);
    job1.setMapperClass(PCYPass1Mapper.class);
    // job1.setCombinerClass(PCYPass1Reducer.class);
    job1.setReducerClass(PCYPass1Reducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + "_1"));
    job1.waitForCompletion(true);
  }
}
