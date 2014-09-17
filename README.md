LogProc
=======
In this project, I implemented different join algorithms from : 

"A Comparison of Join Algorithms for Log Processing in MapReduce" of Spyros Blanas, Jignesh M. Patel, Vuk Ercegovac, Jun Rao, Eugene J. Shekita and Yuanyuan Tian

You can find the paper online at http://www.cs.ucr.edu/~tsotras/cs260/F12/LogProc.pdf, or in [material](/material) directory of this project.

Abbreviation
=======
L: log table \n
R: reference talbe

Standard Repartition Join
=======
It is also the join algorithm provided in the con- tributed join package of Hadoop (org.apache.hadoop.contrib. utils.join). L and R are dynamically partitioned on the join key and the corresponding pairs of partitions are joined. \n