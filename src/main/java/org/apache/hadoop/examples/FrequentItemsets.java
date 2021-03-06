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
  public static int SUPPORT = 200;  // The support for frequent itemsets
  public static double CONFIDENCE = 0.3;  // The confidence for association rules
  public static int HASH_LEN = 100000;  // The length of the hash table
  public static int MAX_RULE_COUNT = 10000;  // The maximum number of rules

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
  /*         "Hash" <hash value> */
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
            valueText.set(keyStr);
          }
          else {
            keyText.set("Freq item");
            valueText.set("(" + keyStr + ":" + String.valueOf(sum) + ")");
          }
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Mapper for preparing for the pass 2 of PCY algorithm */
  /* Input: "Basket" <basket items> */
  /*        "Freq item" <(freq item:count)> */
  /*        "Hash" <hash value> */
  /* Output: "key" B|<basket items> */
  /*         "key" F|<(freq item:count)> */
  /*         "key" H|<hash value> */
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
  /*        "key" H|<hash value> */
  /* Output: <basket items> <list of (freq item:count)>|<list of hash values> */
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

  /* Mapper for the pass 2 of PCY algorithm, to find the frequent pairs */
  /* Input: <basket items> <list of (freq item:count)>|<list of hash values> */
  /* Output: "Basket" <basket items> */
  /*         "Freq item" <list of (freq item:count)> */
  /*         <pair> "1" */
  /*         <hash value> "1" */
  public static class PCYPass2Mapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n\t");

      // Read each line
      while (itr.hasMoreTokens()) {
        String basketStr = itr.nextToken();
        String valueStr = new String("");
        if (itr.hasMoreTokens()) {
          valueStr = itr.nextToken();
        }
        String freqItemStr = valueStr.substring(0, valueStr.indexOf('|'));
        String hashStr = valueStr.substring(valueStr.indexOf('|') + 1);
        String[] freqItemList = freqItemStr.split(",");
        String[] hashList = hashStr.split(",");

        // Preprocess the frequent items list
        for (int i = 0; i < freqItemList.length; i++) {
          freqItemList[i] = freqItemList[i].substring(1, freqItemList[i].indexOf(':'));
        }

        // Output the basket items and the list of frequent items
        keyText.set("Basket");
        valueText.set(basketStr);
        context.write(keyText, valueText);
        keyText.set("Freq item");
        valueText.set(freqItemStr);
        context.write(keyText, valueText);

        String[] items = basketStr.split(",");
        for (int i = 0; i < items.length; i++) {
          for (int j = i + 1; j < items.length; j++) {
            String item1 = items[i];
            String item2 = items[j];
            int i1 = ((item1.hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
            int i2 = ((item2.hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
            int hashPair = (((i1 + i2) % HASH_LEN) + HASH_LEN) % HASH_LEN;
            int freqCount = 0;
            boolean inFreqBucket = false;

            // Read each pair, and add 1 for each candidate pair's count
            for (int f = 0; f < freqItemList.length; f++) {
              if (freqItemList[f].equals(item1) || freqItemList[f].equals(item2)) {
                freqCount++;
              }
            }
            for (int h = 0; h < hashList.length; h++) {
              int hashListNum = Integer.parseInt(hashList[h]);
              if (hashListNum == hashPair) {
                inFreqBucket = true;
              }
            }
            if (freqCount == 2 && inFreqBucket == true) {
              keyText.set(item1 + "+" + item2);
              valueText.set("1");
              context.write(keyText, valueText);
            }

            // Hash each triple of items
            for (int k = j + 1; k < items.length; k++) {
              int i3 = ((items[k].hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
              int hashValue = (((i1 + i2 + i3) % HASH_LEN) + HASH_LEN) % HASH_LEN;
              keyText.set(String.valueOf(hashValue));
              valueText.set("1");
              context.write(keyText, valueText);
            }
          }
        }
      }
    }
  }

  /* Reducer for the pass 2 of PCY algorithm, to find the frequent pairs */
  /* Input: "Basket" <basket items> */
  /*        "Freq item" <list of (freq item:count)> */
  /*        <pair> "1" */
  /*        <hash value> "1" */
  /* Output: "Basket" <basket items> */
  /*         "Freq item" <(freq item:count)> */
  /*         "Freq pair" <(freq pair:count)> */
  /*         "Hash" <hash value> */
  public static class PCYPass2Reducer extends Reducer<Text, Text, Text, Text> {
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String keyStr = key.toString();

      // Output the basket items
      if (keyStr.indexOf('B') != -1) {
        for (Text val : values) {
          context.write(key, val);
        }
      }

      // Output the frequent items
      else if (keyStr.indexOf('F') != -1) {
        for (Text val : values) {
          String[] freqItemList = val.toString().split(",");
          for (int i = 0; i < freqItemList.length; i++) {
            valueText.set(freqItemList[i]);
            context.write(key, valueText);
          }
          break;
        }
      }

      else {

        // Count each bucket or pair
        int sum = 0;
        for (Text val : values) {
          sum += 1;
        }

        // Only output the buckets or items that pass the support
        if (sum >= SUPPORT) {
          if (keyStr.matches("-?\\d+")) {
            keyText.set("Hash");
            valueText.set(keyStr);
          }
          else {
            keyText.set("Freq pair");
            valueText.set("(" + keyStr + ":" + String.valueOf(sum) + ")");
          }
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Mapper for preparing for the pass 3 of PCY algorithm */
  /* Input: "Basket" <basket items> */
  /*        "Freq item" <(freq item:count)> */
  /*        "Freq pair" <(freq pair:count)> */
  /*        "Hash" <hash value> */
  /* Output: "key" B|<basket items> */
  /*         "key" F|<(freq item:count)> */
  /*         "key" P|<(freq pair:count)> */
  /*         "key" H|<hash value> */
  public static class PreparePass3Mapper extends Mapper<Object, Text, Text, Text>{
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

        // Output the key-value pair
        if (typeStr.indexOf('B') != -1 || typeStr.indexOf('H') != -1) {
          keyText.set("key");
          valueText.set(typeStr.substring(0, 1) + "|" + valueStr);
          context.write(keyText, valueText);
        }
        else if (typeStr.indexOf('F') != -1) {
          keyText.set("key");
          if (typeStr.indexOf('p') != -1) {
            valueText.set("P|" + valueStr);
          }
          else {
            valueText.set("F|" + valueStr);
          }
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Reducer for preparing for the pass 3 of PCY algorithm */
  /* Input: "key" B|<basket items> */
  /*        "key" F|<(freq item:count)> */
  /*        "key" P|<(freq pair:count)> */
  /*        "key" H|<hash value> */
  /* Output: <basket items> <list of (freq item:count)>|<list of (freq pair:count)>|<list of hash values> */
  public static class PreparePass3Reducer extends Reducer<Text, Text, Text, Text> {
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String[] baskets = new String[BASKET_COUNT];
      int b = 0;
      String freqItemList = new String("");
      String freqPairList = new String("");
      String hashList = new String("");
      boolean freqItemFirst = true;
      boolean freqPairFirst = true;
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

        // Concatenate all the frequent pairs
        else if (valueType.indexOf('P') != -1) {
          if (!freqPairFirst) {
            freqPairList += ",";
          }
          freqPairList += valueStr;
          freqPairFirst = false;
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
        valueText.set(freqItemList + "|" + freqPairList + "|" + hashList);
        context.write(keyText, valueText);
      }
    }
  }

  /* Mapper for the pass 3 of PCY algorithm, to find the frequent triples */
  /* Input: <basket items> <list of (freq item:count)>|<list of (freq pair:count)>|<list of hash values> */
  /* Output: "Freq item" <list of (freq item:count)> */
  /*         "Freq pair" <list of (freq pair:count)> */
  /*         <triple> "1" */
  public static class PCYPass3Mapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n\t");

      // Read each line
      while (itr.hasMoreTokens()) {
        String basketStr = itr.nextToken();
        String valueStr = new String("");
        if (itr.hasMoreTokens()) {
          valueStr = itr.nextToken();
        }
        int separateIdx1 = valueStr.indexOf('|');
        int separateIdx2 = valueStr.indexOf('|', separateIdx1 + 1);
        String freqItemStr = valueStr.substring(0, separateIdx1);
        String freqPairStr = valueStr.substring(separateIdx1 + 1, separateIdx2);
        String hashStr = valueStr.substring(separateIdx2 + 1);
        String[] freqItemList = freqItemStr.split(",");
        String[] freqPairList = freqPairStr.split(",");
        String[] hashList = hashStr.split(",");

        // Preprocess the frequent items and the frequent pairs lists
        for (int i = 0; i < freqItemList.length; i++) {
          freqItemList[i] = freqItemList[i].substring(1, freqItemList[i].indexOf(':'));
        }
        for (int i = 0; i < freqPairList.length; i++) {
          freqPairList[i] = freqPairList[i].substring(1, freqPairList[i].indexOf(':'));
        }

        // Output the list of frequent items and the list of frequent pairs
        keyText.set("Freq item");
        valueText.set(freqItemStr);
        context.write(keyText, valueText);
        keyText.set("Freq pair");
        valueText.set(freqPairStr);
        context.write(keyText, valueText);

        String[] items = basketStr.split(",");
        for (int i = 0; i < items.length; i++) {
          for (int j = i + 1; j < items.length; j++) {
            for (int k = j + 1; k < items.length; k++) {
              String item1 = items[i];
              String item2 = items[j];
              String item3 = items[k];
              int i1 = ((item1.hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
              int i2 = ((item2.hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
              int i3 = ((item3.hashCode() % HASH_LEN) + HASH_LEN) % HASH_LEN;
              int hashTriple = (((i1 + i2 + i3) % HASH_LEN) + HASH_LEN) % HASH_LEN;
              int freqItemCount = 0;
              int freqPairCount = 0;
              boolean inFreqBucket = false;

              // Read each pair, and add 1 for each candidate pair's count
              for (int f = 0; f < freqItemList.length; f++) {
                if (freqItemList[f].equals(item1) || freqItemList[f].equals(item2) || freqItemList[f].equals(item3)) {
                  freqItemCount++;
                }
              }
              for (int p = 0; p < freqPairList.length; p++) {
                String pairItem1 = freqPairList[p].substring(0, freqPairList[p].indexOf('+'));
                String pairItem2 = freqPairList[p].substring(freqPairList[p].indexOf('+') + 1);
                if ((pairItem1.equals(item1) || pairItem1.equals(item2)) && (pairItem2.equals(item1) || pairItem2.equals(item2))) {
                  freqPairCount++;
                }
                else if ((pairItem1.equals(item1) || pairItem1.equals(item3)) && (pairItem2.equals(item1) || pairItem2.equals(item3))) {
                  freqPairCount++;
                }
                else if ((pairItem1.equals(item2) || pairItem1.equals(item3)) && (pairItem2.equals(item2) || pairItem2.equals(item3))) {
                  freqPairCount++;
                }
              }
              for (int h = 0; h < hashList.length; h++) {
                int hashListNum = Integer.parseInt(hashList[h]);
                if (hashListNum == hashTriple) {
                  inFreqBucket = true;
                }
              }
              if (freqItemCount == 3 && freqPairCount == 3 && inFreqBucket == true) {
                keyText.set(item1 + "+" + item2 + "+" + item3);
                valueText.set("1");
                context.write(keyText, valueText);
              }
            }
          }
        }
      }
    }
  }

  /* Reducer for the pass 3 of PCY algorithm, to find the frequent triples */
  /* Input: "Freq item" <list of (freq item:count)> */
  /*        "Freq pair" <list of (freq pair:count)> */
  /*        <triple> "1" */
  /* Output: "Freq item" <(freq item:count)> */
  /*         "Freq pair" <(freq pair:count)> */
  /*         "Freq triple" <(freq triple:count)> */
  public static class PCYPass3Reducer extends Reducer<Text, Text, Text, Text> {
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String keyStr = key.toString();

      // Output the frequent items and the frequent pairs
      if (keyStr.indexOf('F') != -1) {
        for (Text val : values) {
          String[] freqList = val.toString().split(",");
          for (int i = 0; i < freqList.length; i++) {
            valueText.set(freqList[i]);
            context.write(key, valueText);
          }
          break;
        }
      }

      else {

        // Count each triple
        int sum = 0;
        for (Text val : values) {
          sum += 1;
        }

        // Only output the triples that pass the support
        if (sum >= SUPPORT) {
          keyText.set("Freq triple");
          valueText.set("(" + keyStr + ":" + String.valueOf(sum) + ")");
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Mapper for association rule generation */
  /* Input: "Freq item" <(freq item:count)> */
  /*        "Freq pair" <(freq pair:count)> */
  /*        "Freq triple" <(freq triple:count)> */
  /* Output: "key" R|<rule> */
  /*         "key" F|<(freq item:count)> */
  /*         "key" P|<(freq pair:count)> */
  /*         "key" T|<(freq triple:count)> */
  public static class GenerateRuleMapper extends Mapper<Object, Text, Text, Text>{
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

        // Output the frequent items
        if (typeStr.indexOf('m') != -1) {
          keyText.set("key");
          valueText.set("F|" + valueStr);
          context.write(keyText, valueText);
        }

        // Generate the rules based on frequent pairs and output the pairs
        else if (typeStr.indexOf('a') != -1) {
          String item1 = valueStr.substring(1, valueStr.indexOf('+'));
          String item2 = valueStr.substring(valueStr.indexOf('+') + 1, valueStr.indexOf(':'));
          keyText.set("key");
          valueText.set("R|" + item1 + ">" + item2);
          context.write(keyText, valueText);
          valueText.set("R|" + item2 + ">" + item1);
          context.write(keyText, valueText);
          valueText.set("P|" + valueStr);
          context.write(keyText, valueText);
        }

        // enerate the rules based on frequent triples and output the triples
        else {
          int separateIdx1 = valueStr.indexOf('+');
          int separateIdx2 = valueStr.indexOf('+', separateIdx1 + 1);
          String item1 = valueStr.substring(1, separateIdx1);
          String item2 = valueStr.substring(separateIdx1 + 1, separateIdx2);
          String item3 = valueStr.substring(separateIdx2 + 1, valueStr.indexOf(':'));
          keyText.set("key");
          valueText.set("R|" + item1 + "+" + item2 + ">" + item3);
          context.write(keyText, valueText);
          valueText.set("R|" + item3 + ">" + item1 + "+" + item2);
          context.write(keyText, valueText);
          valueText.set("R|" + item1 + "+" + item3 + ">" + item2);
          context.write(keyText, valueText);
          valueText.set("R|" + item2 + ">" + item1 + "+" + item3);
          context.write(keyText, valueText);
          valueText.set("R|" + item2 + "+" + item3 + ">" + item1);
          context.write(keyText, valueText);
          valueText.set("R|" + item1 + ">" + item2 + "+" + item3);
          context.write(keyText, valueText);
          valueText.set("T|" + valueStr);
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Reducer for association rule generation */
  /* Input: "key" R|<rule> */
  /*        "key" F|<(freq item:count)> */
  /*        "key" P|<(freq pair:count)> */
  /*        "key" T|<(freq triple:count)> */
  /* Output: <rule> <list of (freq item:count)>|<list of (freq pair:count)>|<list of (freq triple:count)> */
  public static class GenerateRuleReducer extends Reducer<Text, Text, Text, Text> {
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String[] rules = new String[MAX_RULE_COUNT];
      int ruleCount = 0;
      String freqItemList = new String("");
      String freqPairList = new String("");
      String freqTripleList = new String("");
      boolean freqItemFirst = true;
      boolean freqPairFirst = true;
      boolean freqTripleFirst = true;

      for (Text val : values) {
        String valueType = val.toString().substring(0, 1);
        String valueStr = val.toString().substring(2);

        // Store all the baskets
        if (valueType.indexOf('R') != -1) {
          rules[ruleCount] = valueStr;
          ruleCount++;
        }

        // Concatenate all the frequent items
        else if (valueType.indexOf('F') != -1) {
          if (!freqItemFirst) {
            freqItemList += ",";
          }
          freqItemList += valueStr;
          freqItemFirst = false;
        }

        // Concatenate all the frequent pairs
        else if (valueType.indexOf('P') != -1) {
          if (!freqPairFirst) {
            freqPairList += ",";
          }
          freqPairList += valueStr;
          freqPairFirst = false;
        }

        // Concatenate all the frequent triples
        else {
          if (!freqTripleFirst) {
            freqTripleList += ",";
          }
          freqTripleList += valueStr;
          freqTripleFirst = false;
        }
      }

      // Output every key-valie pairs
      for (int i = 0; i < ruleCount; i++) {
        keyText.set(rules[i]);
        valueText.set(freqItemList + "|" + freqPairList + "|" + freqTripleList);
        context.write(keyText, valueText);
      }
    }
  }

  /* Mapper for computing the confidence and interrestingness of each rule */
  /* Input: <rule> <list of (freq item:count)>|<list of (freq pair:count)>|<list of (freq triple:count)> */
  /* Output: <confidence> <rule>|<interestingness> */
  public static class FindConfidenceMapper extends Mapper<Object, Text, Text, Text>{
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n\t");

      // Read each line
      while (itr.hasMoreTokens()) {
        String ruleStr = itr.nextToken();
        String valueStr = new String("");
        if (itr.hasMoreTokens()) {
          valueStr = itr.nextToken();
        }
        int separateIdx1 = valueStr.indexOf('|');
        int separateIdx2 = valueStr.indexOf('|', separateIdx1 + 1);
        String freqItemStr = valueStr.substring(0, separateIdx1);
        String freqPairStr = valueStr.substring(separateIdx1 + 1, separateIdx2);
        String freqTripleStr = valueStr.substring(separateIdx2 + 1);
        String[] freqItemList = freqItemStr.split(",");
        String[] freqPairList = freqPairStr.split(",");
        String[] freqTripleList = freqTripleStr.split(",");

        // Find the support for both side of the itemset of each rule
        String[] itemsetBoth = ruleStr.split(">");
        int[] supportBoth = new int[2];
        for (int b = 0; b < 2; b++) {

          // The itemset is a pair
          if (itemsetBoth[b].indexOf('+') != -1) {
            String item1 = itemsetBoth[b].substring(0, itemsetBoth[b].indexOf('+'));
            String item2 = itemsetBoth[b].substring(itemsetBoth[b].indexOf('+') + 1);
            for (int i = 0; i < freqPairList.length; i++) {
              String pairItem1 = freqPairList[i].substring(1, freqPairList[i].indexOf('+'));
              String pairItem2 = freqPairList[i].substring(freqPairList[i].indexOf('+') + 1, freqPairList[i].indexOf(':'));
              if ((pairItem1.equals(item1) && pairItem2.equals(item2)) || (pairItem1.equals(item2) && pairItem2.equals(item1))) {
                String supportStr = freqPairList[i].substring(freqPairList[i].indexOf(':') + 1, freqPairList[i].indexOf(')'));
                supportBoth[b] = Integer.parseInt(supportStr);
                break;
              }
            }
            itemsetBoth[b] = item1 + ", " + item2;
          }

          // The itemset is not a pair
          else {
            for (int i = 0; i < freqItemList.length; i++) {
              String freqItem = freqItemList[i].substring(1, freqItemList[i].indexOf(':'));
              if (freqItem.equals(itemsetBoth[b])) {
                String supportStr = freqItemList[i].substring(freqItemList[i].indexOf(':') + 1, freqItemList[i].indexOf(')'));
                supportBoth[b] = Integer.parseInt(supportStr);
                break;
              }
            }
          }
        }

        // Find the support for the union of both itemsets of each rule
        String[] itemsetUnion = ruleStr.split(">|\\+");
        int supportUnion = 0;

        // The union is a triple
        if (itemsetUnion.length == 3) {
          String item1 = itemsetUnion[0];
          String item2 = itemsetUnion[1];
          String item3 = itemsetUnion[2];
          for (int i = 0; i < freqTripleList.length; i++) {
            int sepIdx1 = freqTripleList[i].indexOf('+');
            int sepIdx2 = freqTripleList[i].indexOf('+', sepIdx1 + 1);
            String tripleItem1 = freqTripleList[i].substring(1, sepIdx1);
            String tripleItem2 = freqTripleList[i].substring(sepIdx1 + 1, sepIdx2);
            String tripleItem3 = freqTripleList[i].substring(sepIdx2 + 1, freqTripleList[i].indexOf(':'));
            if (tripleItem1.equals(item1) || tripleItem1.equals(item2) || tripleItem1.equals(item3)) {
              if (tripleItem2.equals(item1) || tripleItem2.equals(item2) || tripleItem2.equals(item3)) {
                if (tripleItem3.equals(item1) || tripleItem3.equals(item2) || tripleItem3.equals(item3)) {
                  String supportStr = freqTripleList[i].substring(freqTripleList[i].indexOf(':') + 1, freqTripleList[i].indexOf(')'));
                  supportUnion = Integer.parseInt(supportStr);
                  break;
                }
              }
            }
          }
        }

        // The union is a pair
        else {
          String item1 = itemsetUnion[0];
          String item2 = itemsetUnion[1];
          for (int i = 0; i < freqPairList.length; i++) {
            String pairItem1 = freqPairList[i].substring(1, freqPairList[i].indexOf('+'));
            String pairItem2 = freqPairList[i].substring(freqPairList[i].indexOf('+') + 1, freqPairList[i].indexOf(':'));
            if ((pairItem1.equals(item1) && pairItem2.equals(item2)) || (pairItem1.equals(item2) && pairItem2.equals(item1))) {
              String supportStr = freqPairList[i].substring(freqPairList[i].indexOf(':') + 1, freqPairList[i].indexOf(')'));
              supportUnion = Integer.parseInt(supportStr);
              break;
            }
          }
        }

        // Calculate the confidence and interestingness of the rule
        double confidence = (double)(supportUnion) / (double)(supportBoth[0]);
        double interestingness = confidence - (double)(supportBoth[1]) / BASKET_COUNT;

        // Output the key-value pair if the confidence is above threshold
        if (confidence >= CONFIDENCE) {
          String ruleFormatted = "(" + itemsetBoth[0] + ") -> (" + itemsetBoth[1] + ")";
          keyText.set(String.valueOf(1.0 / confidence));
          valueText.set(ruleFormatted + "|" + String.format("%.3f", interestingness));
          context.write(keyText, valueText);
        }
      }
    }
  }

  /* Reducer for computing the confidence and interrestingness of each rule */
  /* Input: <confidence> <rule>|<interestingness> */
  /* Output: <rule> C: <confidence>, I:<interestingness> */
  public static class FindConfidenceReducer extends Reducer<Text, Text, Text, Text> {
    private Text keyText = new Text();
    private Text valueText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String invConfidence = key.toString();
      double confidence = 1.0 / Double.parseDouble(invConfidence);

      for (Text val : values) {
        String ruleStr = val.toString().substring(0, val.toString().indexOf('|'));
        String interestingness = val.toString().substring(val.toString().indexOf('|') + 1);

        // Output the key-valur pair based on the order of the confidence
        keyText.set(ruleStr);
        valueText.set("C: " + String.format("%.3f", confidence) + ", I: " + interestingness);
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
    Job job1 = new Job(conf, "PCY pass 1");
    job1.setJarByClass(FrequentItemsets.class);
    job1.setMapperClass(PCYPass1Mapper.class);
    job1.setReducerClass(PCYPass1Reducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1] + "_1"));
    job1.waitForCompletion(true);

    // Prepare for PCY algorithm pass 2
    Job job2 = new Job(conf, "Prepare pass 2");
    job2.setJarByClass(FrequentItemsets.class);
    job2.setMapperClass(PreparePass2Mapper.class);
    job2.setReducerClass(PreparePass2Reducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs[1] + "_1"));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1] + "_2"));
    job2.waitForCompletion(true);

    // PCY algorithm pass 2: Count the pairs that hash to frequent buckets
    Job job3 = new Job(conf, "PCY pass 2");
    job3.setJarByClass(FrequentItemsets.class);
    job3.setMapperClass(PCYPass2Mapper.class);
    job3.setReducerClass(PCYPass2Reducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(otherArgs[1] + "_2"));
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1] + "_3"));
    job3.waitForCompletion(true);

    // Prepare for PCY algorithm pass 3
    Job job4 = new Job(conf, "Prepare pass 3");
    job4.setJarByClass(FrequentItemsets.class);
    job4.setMapperClass(PreparePass3Mapper.class);
    job4.setReducerClass(PreparePass3Reducer.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job4, new Path(otherArgs[1] + "_3"));
    FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1] + "_4"));
    job4.waitForCompletion(true);

    // PCY algorithm pass 3: Count the triples that hash to frequent buckets
    Job job5 = new Job(conf, "PCY pass 3");
    job5.setJarByClass(FrequentItemsets.class);
    job5.setMapperClass(PCYPass3Mapper.class);
    job5.setReducerClass(PCYPass3Reducer.class);
    job5.setOutputKeyClass(Text.class);
    job5.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job5, new Path(otherArgs[1] + "_4"));
    FileOutputFormat.setOutputPath(job5, new Path(otherArgs[1] + "_freq"));
    job5.waitForCompletion(true);

    // Generate association rules based on the frequent itemsets
    Job job6 = new Job(conf, "Generate rule");
    job6.setJarByClass(FrequentItemsets.class);
    job6.setMapperClass(GenerateRuleMapper.class);
    job6.setReducerClass(GenerateRuleReducer.class);
    job6.setOutputKeyClass(Text.class);
    job6.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job6, new Path(otherArgs[1] + "_freq"));
    FileOutputFormat.setOutputPath(job6, new Path(otherArgs[1] + "_6"));
    job6.waitForCompletion(true);

    // Calculate the confidence and the interestingness of each rule
    Job job7 = new Job(conf, "Confidence and interestingness");
    job7.setJarByClass(FrequentItemsets.class);
    job7.setMapperClass(FindConfidenceMapper.class);
    job7.setReducerClass(FindConfidenceReducer.class);
    job7.setOutputKeyClass(Text.class);
    job7.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job7, new Path(otherArgs[1] + "_6"));
    FileOutputFormat.setOutputPath(job7, new Path(otherArgs[1] + "_rule"));
    job7.waitForCompletion(true);
  }
}
