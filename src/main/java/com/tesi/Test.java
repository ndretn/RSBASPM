package com.tesi;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Class to perform the test to verify if the set of sequential patterns extracted from the sample
 * is an eps-approximation
 */
public class Test
{
    HashMap<String,Integer> miningTheta;
    HashMap<String,Integer> miningThetaMinusEpsilon;
    HashMap<String,Integer> miningSample;
    int sizeData;
    int sizeSample;
    int numTest;
    int numEpsilonApp;
    double epsion;
    String fileTheta;
    String fileThetaMinusEpsilon;

    /**
     * Constructor of the class
     * @param fileTheta file containing the sequential patterns extracted from the whole dataset using theta
     * @param fileThetaMinusEpsilon file containing the sequential patterns extracted from the whole dataset
     *                              using theta minus epsilon
     * @param sizeData the size of the whole dataset
     * @param sizeSample the size of the sample
     * @param epsilon the error threshold
     */
    Test(String fileTheta, String fileThetaMinusEpsilon, int sizeData, int sizeSample, double epsilon)
    {
        this.sizeData = sizeData;
        this.sizeSample = sizeSample;
        this.epsion = epsilon;
        this.fileTheta = fileTheta;
        this.fileThetaMinusEpsilon = fileThetaMinusEpsilon;
        numTest = 0;
        numEpsilonApp = 0;
        miningTheta = new HashMap<>();
        miningThetaMinusEpsilon = new HashMap<>();
        loadData();
    }

    /**
     * Loads the data
     */
    public void loadData()
    {
        try
        {
            File f = new File(fileTheta);
            Scanner sc = new Scanner(f);
            while (sc.hasNextLine())
            {
                String line = sc.nextLine();
                String[] s = line.split(" #SUP: ");
                miningTheta.put(s[0],Integer.parseInt(s[1]));
            }
            sc.close();
            System.gc();
        }
        catch (FileNotFoundException e)
        {
            System.err.println("File " + fileTheta + " Not Found!\n" + e);
        }
        try
        {
            File f = new File(fileThetaMinusEpsilon);
            Scanner sc = new Scanner(f);
            while (sc.hasNextLine())
            {
                String line = sc.nextLine();
                String[] s = line.split(" #SUP: ");
                miningThetaMinusEpsilon.put(s[0],Integer.parseInt(s[1]));
            }
            sc.close();
            System.gc();
        }
        catch (FileNotFoundException e)
        {
            System.err.println("File " + fileThetaMinusEpsilon + " Not Found!\n" + e);
        }
    }

    /**
     * Performs a test on a set of sequential patterns extracted from a single sample and prints
     * some statistics
     * @param fileTest file containing the sequential patterns extracted from the sample using
     *                 theta minus half epsilon
     * @return true if the set extracted is an eps-approximation
     *
     */
    public boolean doTest(String fileTest)
    {
        if(miningTheta.size() == 0 || miningThetaMinusEpsilon.size() == 0)
        {
            System.out.println("First Execute loadData()!");
            return false;
        }
        miningSample = new HashMap<>();
        numTest++;
        try
        {
            File f = new File(fileTest);
            Scanner sc = new Scanner(f);
            while (sc.hasNextLine())
            {
                String line = sc.nextLine();
                String[] s = line.split(" #SUP: ");
                miningSample.put(s[0],Integer.parseInt(s[1]));
            }
            sc.close();
            System.gc();
        }
        catch (FileNotFoundException e)
        {
            System.err.println("File " + fileTest + " Not Found!\n" + e);
        }
        for(String s : miningTheta.keySet())
        {
            if(!miningSample.containsKey(s)) return false;
        }
        double differenceValue[] = new double[miningSample.size()];
        double max = 0d;
        double min = 1d;
        double sum = 0d;
        double q1 = 0d;
        double q2 = 0d;
        double q3 = 0d;
        int i = 0;
        for(String s: miningSample.keySet())
        {
            if(miningThetaMinusEpsilon.containsKey(s))
            {
                differenceValue[i] = Math.abs(miningSample.get(s)/ (double) sizeSample - miningThetaMinusEpsilon.get(s) / (double) sizeData);
                if(differenceValue[i] > epsion) return false;
                if(differenceValue[i]>max) max = differenceValue[i];
                if(differenceValue[i]<min) min = differenceValue[i];
                sum+=differenceValue[i++];
            }
        }
        numEpsilonApp++;
        sum/=miningSample.size();
        double variance = 0;
        for(int j=0;j<miningSample.size();j++)
        {
            variance = Math.pow(differenceValue[j]-sum,2.);
        }
        variance/=miningSample.size();
        Arrays.sort(differenceValue);
        int size = differenceValue.length;
        if(size%2==0) q2 = (differenceValue[size/2]+differenceValue[size/2-1])/2.;
        else q2 = differenceValue[size/2];
        if((size/2)%2!=0)
        {
            q1 = differenceValue[size / 4];
            q3 = differenceValue[size / 2 + size / 4 + 1];
        }
        else
        {
            q1 = (differenceValue[size/4] + differenceValue[size/4-1])/2.;
            q3 = (differenceValue[size/2+size/4] + differenceValue[size/2+size/4-1])/2.;
        }
        System.out.println("Mean: "+ sum + " - Variance: " + variance + " - Max: " + max +" - Min: " + min + " - Q1: " + q1 + " - Q2: " + q2 + " - Q3: " + q3);
        return true;
    }

    /**
     * Computes the probability of having eps-approximation in different sample
     * @return the probability of having an eps-approximation
     */
    public float getProbability()
    {
        return numEpsilonApp/(float)numTest;
    }
}