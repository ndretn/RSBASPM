package com.tesi;

import ca.pfv.spmf.tools.MemoryLogger;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.PrefixSpan;
import org.apache.spark.mllib.fpm.PrefixSpanModel;
import java.io.*;
import java.util.List;

/**
 * Class to perform the extraction of frequent sequential patterns using the Spark Framework
 * and the PrefixSpan Algorithm
 */
public class SparkSP
{
    String inputFile;
    String outputFile;
    JavaRDD<ObjectArrayList<IntArrayList>> dataset;
    int patternCount;
    double theta;
    long startTime;
    long endTime;
    JavaSparkContext scc;

    /**
     * Constructor of the class
     * @param inputFile name of the dataset file
     * @param outputFile name of the output file
     * @param scc the Java Spark Context
     */
    SparkSP(String inputFile, String outputFile, JavaSparkContext scc)
    {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        patternCount = 0;
        this.scc = scc;
    }

    /**
     * Executes the mining from the dataset and writes the results and the statistics in a file
     * @param theta the frequency threshold
     * @param maxLength the size of the largest sequential pattern to find
     */
    public void execute(double theta, int maxLength)
    {
        this.theta=theta;
        MemoryLogger.getInstance().reset();
        startTime = System.currentTimeMillis();
        dataset = scc.textFile(inputFile).map(s->
        {
            ObjectArrayList<IntArrayList> sp = new ObjectArrayList<>();
            String[] split = s.split(" ");
            IntArrayList itemset = new IntArrayList();
            for(String ss: split)
            {
                if(Integer.parseInt(ss)>=0) itemset.add(Integer.parseInt(ss));
                else if(Integer.parseInt(ss)==-1)
                {
                    sp.add(itemset);
                    itemset = new IntArrayList();
                }
            }
            return sp;
        });
        PrefixSpan prefixSpan = new PrefixSpan()
                .setMinSupport(theta)
                .setMaxPatternLength(maxLength);

        PrefixSpanModel<Integer> model = prefixSpan.run(dataset);
        try
        {
            BufferedWriter output = new BufferedWriter(new FileWriter(outputFile));
            StringBuilder r = new StringBuilder();
            for (PrefixSpan.FreqSequence<Integer> freqSeq : model.freqSequences().toJavaRDD().collect())
            {
                for (List<Integer> it : freqSeq.javaSequence())
                {
                    for (int j : it)
                    {
                        r.append(j + " ");
                    }
                    r.append("-1 ");
                }
                r.append("#SUP: "+ freqSeq.freq() + "\n");
                patternCount++;
            }
            output.write(r.toString());
            output.close();
        }
        catch (IOException e)
        {
            System.err.println("Writing Error! " + e);
        }
        endTime = System.currentTimeMillis();
        MemoryLogger.getInstance().checkMemory();
        printStatistics();
    }

    /**
     * Private method that prints some statistics about the execution
     */
    private void printStatistics()
    {
        StringBuilder r = new StringBuilder(200);
        r.append("=============  PREFIXSPAN SPARK - STATISTICS =============\n Total time ~ ");
        r.append(endTime - startTime);
        r.append(" ms\n");
        r.append(" Frequent sequences count : " + patternCount);
        r.append('\n');
        r.append(" Max memory (mb) : ");
        r.append(MemoryLogger.getInstance().getMaxMemory());
        r.append('\n');
        r.append(" minfreq = " + theta + ".");
        r.append('\n');
        r.append("===================================================\n");
        System.out.println(r.toString());
    }
}

