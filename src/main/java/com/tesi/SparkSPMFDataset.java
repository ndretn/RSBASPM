package com.tesi;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;


/**
 * Utility class to perform operation in the dataset using the Apache Spark Framework
 */
public class SparkSPMFDataset
{

    String inputFile;
    JavaRDD<String> dataset;
    List<String> sample;
    int datasetSize;
    int sampleSize;
    JavaSparkContext scc;
    static int ct = 0;

    /**
     * Constructor of the class
     * @param inputFile the name of the dataset file
     * @param scc the Java Spark Context
     */
    SparkSPMFDataset(String inputFile, JavaSparkContext scc)
    {
        this.inputFile = inputFile;
        datasetSize = 0;
        this.scc = scc;
    }

    /**
     * Loads the dataset
     */
    public void loadDataset()
    {
        dataset = scc.textFile(inputFile);
        datasetSize = dataset.collect().size();
    }

    /**
     * Returns the size of the dataset
     * @return the size of the dataset
     */
    public int getDatasetSize()
    {
        return datasetSize;
    }

    /**
     * Replicates the whole dataset multiple times and writes it in a new file
     * @param file the name of the output file for the savage of the replicated dataset
     * @param numberRepetition the number of times that the dataset is replicated
     */
    public void replicate(String file, int numberRepetition)
    {
        try
        {
            BufferedWriter output = new BufferedWriter(new FileWriter(file));
            for (String s : dataset.collect())
            {
                StringBuilder r = new StringBuilder();
                for (int i = 0; i < numberRepetition; i++)
                {
                    r.append(s + "\n");
                }
                output.write(r.toString());
            }
            output.close();
        }
        catch (IOException e)
        {
            System.err.println("Writing Error!\n" + e);
        }
    }

    /**
     * Takes a sample from the dataset and writes it in a new file
     * @param size the sample size
     * @param outputFile the name of the output file for the savage of the sample
     * @param replacement if true the sample is taken with replacement
     */
    public void sample(int size, String outputFile, boolean replacement)
    {
        sample = dataset.takeSample(replacement, size, System.currentTimeMillis());
        try
        {
            BufferedWriter output = new BufferedWriter(new FileWriter(outputFile));
            for (String s : sample)
            {
                output.write(s + "\n");
            }
            output.close();
            sample = null;
        }
        catch (IOException e)
        {
            System.err.println("Writing Error!\n" + e);
        }
    }

    /**
     * Computes and returns the sample size, print the d-bound or f-bound
     * @param c the c constant
     * @param epsilon the error threshold
     * @param delta the probability threshold
     * @param originalLength if true the number of items is used as length of a sequence;
     *                       otherwise it is used the number of itemsets
     * @return the sample size computed
     */
    public int sampleSize(double c,double epsilon,double delta,boolean originalLength)
    {
        int q = 0;
        JavaPairRDD<Integer, String> data = dataset.distinct().mapToPair(s -> new Tuple2<>(getLength(s,originalLength), s)).sortByKey(false);
        List<Tuple2<Integer, String>> d = data.collect();
        int i;
        for (i = 0; i < d.size() && d.get(i)._1() > q; i++)
        {
            q++;
        }
        if(originalLength) System.out.println("d-bound: " + q);
        else
        {
            System.out.println("f-bound: " + q);
            q*=ct;
        }
        sampleSize = Math.min(datasetSize,(int)(4*c/(epsilon*epsilon)*(q+Math.log(1/delta))));
        d = null;
        data = null;
        return sampleSize;
    }

    /**
     * Private static method to compute the size of a sequential patterns represented as a string in SPMF format
     * @param s the sequential pattern
     * @param originalLength if true the number of items is used as length of a sequence;
     *                       otherwise it is used the number of itemsets
     * @return the size of the sequential pattern
     */
    private static int getLength(String s,boolean originalLength)
    {
        int length = 0;
        int c = 0;
        String[] split = s.split(" ");
        for (String ss : split)
        {
            if (originalLength && Integer.parseInt(ss) >= 0) length++;
            if(!originalLength)
            {
                if(Integer.parseInt(ss)>=0) c++;
                if(Integer.parseInt(ss) == -1)
                {
                    length++;
                    if(c>ct) ct = c;
                    c = 0;
                }
            }
        }
        return length;
    }

    /**
     * Prints some statistics about the dataset
     * @param originalLength if true the number of items is used as length of a sequence;
     *                       otherwise it is used the number of itemsets
     */
    public void analyseData(boolean originalLength)
    {
        double avgLength = 0.0d;
        double stdLength = 0.0d;
        int maxLength = 0;
        JavaPairRDD<Integer, String> data = dataset.distinct().mapToPair(s -> new Tuple2<>(getLength(s,originalLength), s)).sortByKey(false);
        List<Tuple2<Integer, String>> d = data.collect();
        for (int i = 0; i < d.size(); i++)
        {
            int temp = d.get(i)._1;
            if(temp > maxLength) maxLength = temp;
            avgLength+=temp;
        }
        avgLength/=datasetSize;
        for (int i = 0; i < d.size(); i++)
        {
            stdLength+=Math.pow(d.get(i)._1-avgLength,2);
        }
        stdLength/=datasetSize;
        stdLength = Math.sqrt(stdLength);
        System.out.println("Dataset Size: " + datasetSize);
        System.out.println("AVG Sequential Transactions Length: " + avgLength);
        System.out.println("STD Sequential Transactions Length: " + stdLength);
        System.out.println("MAX Sequential Transactions Length: " + maxLength);
    }

    /**
     * Frees space and stop the Java Spark Context
     */
    public void close()
    {
        dataset = null;
        scc.stop();
    }
}


