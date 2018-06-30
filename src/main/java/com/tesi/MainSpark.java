package com.tesi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class MainSpark
{
    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName("SparkSP");
        JavaSparkContext scc = new JavaSparkContext(sparkConf);
        String dataset = "MSNBC_SPMF";
        System.out.println("Dataset: " + dataset);
        double delta = 0.1d;
        double epsilon = 0.01d;
        double c = 0.1d;
        double theta = 0.02;
        int maxLength = 1000000;
        boolean originalLength = true;
        boolean replacement = true;
        int n = 50;
        SparkSPMFDataset dat = new SparkSPMFDataset("data/"+dataset+".txt",scc);
        dat.loadDataset();
        int datasetSize = dat.getDatasetSize();
        System.out.println("Dataset Size: " + datasetSize);
        int sampleSize = dat.sampleSize(c,epsilon,delta,originalLength);
        System.out.println("Sample Size: " + sampleSize);
        for(int i=0;i<n;i++)
        {
            dat.sample(sampleSize,"data/"+dataset+"_S"+i+".txt",replacement);
        }
        dat = null;
        SparkSP sp = new SparkSP("data/"+dataset+".txt","data/"+dataset+"_OUT.txt",scc);
        sp.execute(theta,maxLength);
        sp = new SparkSP("data/"+dataset+".txt","data/"+dataset+"_OUTEPS.txt",scc);
        sp.execute(theta-epsilon,maxLength);
        for(int i=0;i<n;i++)
        {
            sp = new SparkSP("data/"+dataset+"_S"+i+".txt","data/"+dataset+"_S"+i+"_OUT.txt",scc);
            sp.execute(theta-epsilon/2,maxLength);
        }
        sp = null;
        Test t = new Test("data/"+dataset+"_OUT.txt","data/"+dataset+"_OUTEPS.txt",datasetSize,sampleSize,epsilon);
        for(int i=0;i<n;i++)
        {
            t.doTest("data/"+dataset+"_S"+i+"_OUT.txt");
        }
        System.out.println("EPS-Approximation Probability: " + t.getProbability());
    }

}
