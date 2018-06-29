# RSBASPM: A Rigorous Sampling-Based Approach for Sequential Pattern Mining

This repository contains the code developed for my master's thesis in Computer Engineering at University of Padova,
supervised by [Professor Fabio Vandin](http://www.dei.unipd.it/~vandinfa/).

The focus for this work was to find an upper bound on the sample size required for sequential pattern mining, while
providing rigorous guarantees on the results obtained from the sample with respect to the results that would be 
obtained from the whole dataset.

This project is copyright of Andrea Tonon and it licensed under the Apache License 2.0, as also described in the file [LICENSE](LICENSE).

## Project structure ##

The code for this project has been developed using IntelliJ IDE, JAVA, the Apache Spark Framework and the [SPMF library]
(http://www.philippe-fournier-viger.com/spmf/).

The class MainSPMF and MainSpark provide an usage example, respectively of the sequential and paralled/distributed versions,
with the MSNBC_SPMF dataset provided in the Data folder.

Required VM options:
* -XmxRG: allows to specify the maximum memory allocation pool for a Java virtual machine (JVM), where R must be replaced with
an integer that represents the maximum memory in GB.
* -Dspark.master="local\[X\]": only for the Spark version, X must be repleaced with the number of cores of your machine 

## License

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
