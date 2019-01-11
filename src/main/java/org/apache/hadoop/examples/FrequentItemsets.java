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

  public static int BASKET_COUNT = 9835;  // The total number of baskets
  public static int SUPPORT = 100;  // The support for frequent itemsets
  public static int HASH_LEN = 100000;  // The length of the hash table

  /* Mapper for the pass 1 of PCY algorithm, to find the frequent pairs */
  /* Input: <basket items> */
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
  /*         "Freq item" <(freq item:count)> */
  /*         "Hash" <(hash value:count)> */
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
          valueText.set("(" + keyStr + ":" + String.valueOf(sum) + ")");
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Mapper for preparing for the pass 2 of PCY algorithm */
  /* Input: "Basket" <basket items> */
  /*        "Freq item" <(freq item:count)> */
  /*        "Hash" <(hash value:count)> */
  /* Output: "key" B|<basket items> */
  /*         "key" F|<(freq item:count)> */
  /*         "key" H|<(hash value:count)> */
  public static class PreparePass2Mapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n\t");

      // Read each line
      while (itr.hasMoreTokens()) {
        String typeStr = itr.nextToken();
        String valueStr = new String("");
        if (itr.hasMoreTokens()) {
          valueStr = itr.nextToken();
        }

        if (typeStr.indexOf('B') != -1 || typeStr.indexOf('F') != -1 || typeStr.indexOf('H') != -1) {

          // Output the key-value pair
          keyText.set("key");
          valueText.set(typeStr.substring(0, 1) + "|" + valueStr);
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Reducer for preparing for the pass 2 of PCY algorithm */
  /* Input: "key" B|<basket items> */
  /*        "key" F|<(freq item:count)> */
  /*        "key" H|<(hash value:count)> */
  /* Output: <basket items> <list of (freq item:count)>|<list of (hash value:count)> */
  public static class PreparePass2Reducer extends Reducer<Text, Text, Text, Text> {
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String[] baskets = new String[BASKET_COUNT];
      int b = 0;
      String freqItemList = new String("");
      String hashList = new String("");
      boolean freqItemFirst = true;
      boolean hashFirst = true;

      for (Text val : values) {
        String valueType = val.toString().substring(0, 1);
        String valueStr = val.toString().substring(2);

        // Store all the baskets
        if (valueType.indexOf('B') != -1) {
          baskets[b] = valueStr;
          b++;
        }

        // Concatenate all the frequent items
        else if (valueType.indexOf('F') != -1) {
          if (!freqItemFirst) {
            freqItemList += ",";
          }
          freqItemList += valueStr;
          freqItemFirst = false;
        }

        // Concatenate all the hash values
        else {
          if (!hashFirst) {
            hashList += ",";
          }
          hashList += valueStr;
          hashFirst = false;
        }
      }

      // Output every key-valie pairs
      for (int i = 0; i < BASKET_COUNT; i++) {
        keyText.set(baskets[i]);
        valueText.set(freqItemList + "|" + hashList);
        context.write(keyText, valueText);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: frequentitemsets <in-file> <out-dir>");
      System.exit(2);
    }

    // PCY algorithm pass 1: Count the items, and hash each pair
    /*
    Job job1 = new Job(conf, "PCY pass 1");
    job1.setJarByClass(FrequentItemsets.class);
    job1.setMapperClass(PCYPass1Mapper.class);
    // job1.setCombinerClass(PCYPass1Reducer.class);
    job1.setReducerClass(PCYPass1Reducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + "_1"));
    job1.waitForCompletion(true);
    */

    // Prepare for PCY algorithm pass 2
    Job job2 = new Job(conf, "Prepare pass 2");
    job2.setJarByClass(FrequentItemsets.class);
    job2.setMapperClass(PreparePass2Mapper.class);
    // job2.setCombinerClass(PCYPass1Reducer.class);
    // job2.setNumReduceTasks(0);
    job2.setReducerClass(PreparePass2Reducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs[1] + "_1"));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "_2"));
    job2.waitForCompletion(true);
  }
}
