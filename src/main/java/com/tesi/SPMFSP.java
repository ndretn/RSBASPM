package com.tesi;

import ca.pfv.spmf.algorithms.sequentialpatterns.prefixspan.AlgoPrefixSpan;
import java.io.IOException;

/**
 * Class to perform the extraction of frequent sequential patterns using the SPMF Framework
 * and the PrefixSpan Algorithm
 */
public class SPMFSP
{
    String inputFile;
    String outputFile;
    AlgoPrefixSpan algo;

    /**
     * Constructor of the class
     * @param inputFile name of the dataset file
     * @param outputFile name of the output file
     */
    SPMFSP(String inputFile, String outputFile)
    {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    /**
     * Executes the mining from the dataset and writes the results and the statistics in a file
     * @param theta the frequency threshold
     * @param maxLength the size of the largest sequential pattern to find
     */
    public void execute(double theta, int maxLength)
    {
        algo = new AlgoPrefixSpan();
        try
        {
            algo.setMaximumPatternLength(maxLength);
            algo.runAlgorithm(inputFile, theta, outputFile);
            algo.printStatistics();
        }
        catch (IOException e)
        {
            System.err.println("File Error!\n" + e);
        }
    }
}