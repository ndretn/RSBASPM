package com.tesi;

import ca.pfv.spmf.algorithms.sequentialpatterns.clofast.AlgoCloFast;
import java.io.IOException;

/**
 * Class to perform the extraction of closed sequential patterns using the SPMF Framework
 * and the CloFast Algorithm
 */
public class CSP
{
    String inputFile;
    String outputFile;
    AlgoCloFast algo;

    /**
     * Constructor of the class
     * @param inputFile name of the dataset file
     * @param outputFile name of the output file
     */
    CSP(String inputFile, String outputFile)
    {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    /**
     * Executes the mining from the dataset and writes the results and the statistics in a file
     * @param theta the frequency threshold
     */
    public void execute(float theta)
    {
        try
        {
            algo.runAlgorithm(inputFile,outputFile,theta);
            algo.printStatistics();
        }
        catch (IOException e)
        {
            System.err.println("File Error!\n" + e);
        }
    }
}