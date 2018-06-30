package com.tesi;

public class MainSPMF {
    public static void main(String[] args)
    {
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
        SPMFDataset dat = new SPMFDataset("data/"+dataset+".txt");
        int sampleSize = dat.loadDatasetAndSampleSize(c,epsilon,delta,originalLength);
        int datasetSize = dat.getDatasetSize();
        System.out.println("Dataset Size: " + datasetSize);
        System.out.println("Sample Size: " + sampleSize);
        for(int i=0;i<n;i++)
        {
            dat.sample(sampleSize,"data/"+dataset+"_S"+i+".txt",replacement);
        }
        dat = null;
        SPMFSP sp = new SPMFSP("data/"+dataset+".txt","data/"+dataset+"_OUT.txt");
        sp.execute(theta,maxLength);
        sp = new SPMFSP("data/"+dataset+".txt","data/"+dataset+"_OUTEPS.txt");
        sp.execute(theta-epsilon,maxLength);
        for(int i=0;i<n;i++)
        {
            sp = new SPMFSP("data/"+dataset+"_S"+i+".txt","data/"+dataset+"_S"+i+"_OUT.txt");
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