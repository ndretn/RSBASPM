package com.tesi;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import javafx.util.Pair;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Utility class to perform operation in the dataset
 */
public class SPMFDataset
{
    int sample[];
    ObjectArrayList<String> dataset;
    String file;
    int datasetSize;
    int sampleSize;
    int ct;

    /**
     * Constructor of the class
     * @param file the name of the dataset file
     */
    SPMFDataset(String file)
    {
        this.file = file;
        dataset = new ObjectArrayList<>();
        datasetSize = 0;
        ct = 0;
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
     * Loads the dataset
     */
    public void loadDataset()
    {
        try
        {
            File f = new File(file);
            Scanner sc = new Scanner(f);
            while (sc.hasNextLine())
            {
                dataset.add(datasetSize++, sc.nextLine());
            }
            sc.close();
            System.gc();
        }
        catch (FileNotFoundException e)
        {
            System.err.println("File Not Found!\n" + e);
        }
    }

    /**
     * Loads the dataset and computes and returns the sample size
     * @param c the c constant
     * @param epsilon the error threshold
     * @param delta the probability threshold
     * @param originalLength if true the number of items is used as length of a sequence;
     *                       otherwise it is used the number of itemsets
     * @return the sample size computed
     */
    public int loadDatasetAndSampleSize(double c, double epsilon, double delta, boolean originalLength)
    {
        ObjectArrayList<Pair<String,Integer>> s = new ObjectArrayList<>();
        HashSet<String> ss = new HashSet();
        int i=0;
        int q = 1;
        try
        {
            File f = new File(file);
            Scanner sc = new Scanner(f);
            String value = sc.nextLine();
            int minLength = getLength(value,originalLength);
            dataset.add(datasetSize++, value);
            s.add(new Pair<>(value,minLength));
            ss.add(value);
            while (sc.hasNextLine())
            {
                value = sc.nextLine();
                dataset.add(datasetSize++, value);
                int l = getLength(value,originalLength);
                if (l > q)
                {
                    if (!ss.contains(value))
                    {
                        s.add(new Pair<>(value,l));
                        ss.add(value);
                        s.sort((o1,o2)-> (o1.getValue()<o2.getValue())?1:-1);
                        if(l < minLength) minLength = l;
                        if(minLength < s.size()) q = minLength;
                        else q = s.size();
                        for (int j = q; j < s.size(); j++)
                        {
                            ss.remove(s.get(j).getKey());
                            s.remove(j);
                        }
                        minLength = s.get(q-1).getValue();
                    }
                }
            }
            sc.close();
            System.gc();
        }
        catch (FileNotFoundException e)
        {
            System.err.println("File Not Found!\n" + e);
        }
        if(originalLength) System.out.println("d-bound: " + q);
        else
        {
            System.out.println("f-bound: " + q);
            q*=ct;
        }
        sampleSize = Math.min(dataset.size(),(int)(4*c/(epsilon*epsilon)*(q+Math.log(1/delta))));
        s = null;
        ss = null;
        return sampleSize;
    }

    /**
     * Private static method to compute the size of a sequential patterns represented as a string in SPMF format
     * @param s the sequential pattern
     * @param originalLength if true the number of items is used as length of a sequence;
     *                       otherwise it is used the number of itemsets
     * @return the size of the sequential pattern
     */
    private int getLength(String s, boolean originalLength)
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
     * Replicates the whole dataset multiple times and writes it in a new file
     * @param file the name of the output file for the savage of the replicated dataset
     * @param numberRepetition the number of times that the dataset is replicated
     */
    public void replicate(String file, int numberRepetition)
    {
        try
        {
            FileWriter output = new FileWriter(file);
            for (int i = 0; i < datasetSize; i++)
            {
                for (int r = 0; r < numberRepetition; r++)
                {
                    output.write(dataset.get(i) + "\n");
                }
            }
            output.close();
            System.gc();
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
        sample = new int[size];
        Random randomGenerator = new Random();
        if(!replacement)
        {
            for (int i = 0; i < datasetSize; i++)
            {
                if (i < size)
                {
                    sample[i] = i;
                }
                else
                {
                    int r = randomGenerator.nextInt(i + 1);
                    if (r < size)
                    {
                        sample[r] = i;
                    }
                }
            }
        }
        else
        {
            for(int i=0;i<size;i++) sample[i] = randomGenerator.nextInt(datasetSize);
        }
        try
        {
            FileWriter output = new FileWriter(outputFile);
            for (int i = 0; i < size; i++)
            {
              output.write(dataset.get(sample[i]) + "\n");
            }
            output.close();
            sample = null;
            System.gc();
        }
        catch (IOException e)
        {
            System.err.println("Writing Error!\n" + e);
        }
    }

    /**
     * Takes a sample from the dataset using less memory and writes it in a new file.
     * It doesn't require the load of the dataset. Useful for the creation of multiple
     * sample with large dataset
     * @param dataSize the whole dataset size
     * @param sampleSize the sample size
     * @param outputFile the name of the output file for the savage of the sample
     * @param replacement if true the sample is taken with replacement
     */
    public void lowMemorySample(int dataSize, int sampleSize, String outputFile, boolean replacement)
    {
        sample = new int[sampleSize];
        Random randomGenerator = new Random();
        if(!replacement)
        {
            for (int i = 0; i < dataSize; i++)
            {
                if (i < sampleSize)
                {
                    sample[i] = i;
                }
                else
                {
                    int r = randomGenerator.nextInt(i + 1);
                    if (r < sampleSize)
                    {
                        sample[r] = i;
                    }
                }
            }
        }
        else
        {
            for(int i=0;i<sampleSize;i++) sample[i] = randomGenerator.nextInt(datasetSize);
        }
        Arrays.sort(sample);
        try
        {
            int row = 0;
            FileWriter output = new FileWriter(outputFile);
            File f = new File(file);
            Scanner sc = new Scanner(f);
            for (int i = 0; i < sampleSize; i++)
            {
                while (row++<sample[i])
                {
                    sc.nextLine();
                }
                output.write(sc.nextLine() + "\n");
            }
            output.close();
            sc.close();
            System.gc();
        }
        catch (IOException e)
        {
            System.err.println("Writing Error!\n" + e);
        }
    }
}
