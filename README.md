LogProc
=======
In this project, I implemented different join algorithms from: 

"A Comparison of Join Algorithms for Log Processing in MapReduce" of Spyros Blanas, Jignesh M. Patel, Vuk Ercegovac, Jun Rao, Eugene J. Shekita and Yuanyuan Tian

You can find the paper online at http://www.cs.ucr.edu/~tsotras/cs260/F12/LogProc.pdf, or in [material](/material) directory of this project.

Abbreviation
=======
L: log table

R: reference talbe

Standard Repartition Join
=======
It is also the join algorithm provided in the con- tributed join package of Hadoop **(org.apache.hadoop.contrib. utils.join)**. L and R are dynamically partitioned on the join key and the corresponding pairs of partitions are joined. 

	Map phase: 
		- Each map task works on a split of either R or L, uses tagging to identify with is original table
		- Input: (K: null, V : a record from a split of either R or L)
		- Output: (join key, tagged record)

	Reduce phase:
		- Input: (K': join key, LIST V': records from R and L with join key K')
		- Performs a cross-product between records in set of R and L.
		- Output: (null, r*l) which r*l is joined record

	Potential problems: When the key cardinality is small or when the data is highly skewed, all the records for a given join key may not fit in memory.
