package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class AppReducer extends Reducer<Text, Iterable<VIntWritable>, Text, IntWritable> {

    @Override
    protected void reduce(Text word, Iterable<Iterable<VIntWritable>> value, Context context) throws IOException, InterruptedException {

        Iterator iterator = value.iterator();
        int count = 0;
        while(iterator.hasNext()){
            IntWritable i = (IntWritable)iterator.next();
            count += i.get();
        }

        context.write(word, new IntWritable(count));
    }
}
