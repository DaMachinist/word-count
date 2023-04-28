package org.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // créer la configuration
        Configuration conf = new Configuration();

        // créer le job
        Job job = Job.getInstance(conf, "Word count");
        //définir la classe principale
        job.setJarByClass(Main.class);
        //définir la classe mapper
        job.setMapperClass(AppMapper.class);
        //définir la classe reducer
        job.setReducerClass(AppReducer.class);
        //définir le type du clé output
        job.setOutputKeyClass(Text.class);
        //définir le type de la valeur output
        job.setOutputValueClass(IntWritable.class);
        //Ajouter le fichier à traiter
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
