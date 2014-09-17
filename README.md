LogProc
=======
In this project, I implemented different join algorithms from: 

**"A Comparison of Join Algorithms for Log Processing in MapReduce"** of *Spyros Blanas, Jignesh M. Patel, Vuk Ercegovac, Jun Rao, Eugene J. Shekita and Yuanyuan Tian*

You can find the paper online at http://www.cs.ucr.edu/~tsotras/cs260/F12/LogProc.pdf, or in [material](/material) directory of this project.

**Abbreviation**: **L** is log table and **R** is reference table

Standard Repartition Join
=======
It is also the join algorithm provided in the con- tributed join package of Hadoop **(org.apache.hadoop.contrib. utils.join)**. L and R are dynamically partitioned on the join key and the corresponding pairs of partitions are joined. 

	Map phase: 
		- Each map task works on a split of either R or L, uses tagging to identify with is original table
		- Input: (K: null, V : a record from a split of either R or L)
		- Output: (join key, tagged record)

	Reduce phase:
		- Input: (K': join key, LIST V': records from R and L with join key K')
		- Buffer records from R (Br) and buffer records from L (Bl)
		- Performs a cross-product between records in set of Br and Bl.
		- Output: (null, r*l) which r*l is joined record

**Potential problem:** When the key cardinality is small or when the data is highly skewed, all the records for a given join key may not fit in memory (Br + Bl, mostly Bl).

Improved Repartition Join
=======
To fix the buffering problem of the standard repartition join which is Bl too large.

	Map phase: 
		- Each map task works on a split of either R or L, uses tagging to identify with is original table
		- Input: (K: null, V : a record from a split of either R or L)
		- Output: (composite_key, tagged record) with composite_key = (join key, tag)

	Partiion phase: 
		- Input: composite_key K
		- Output: partition of K which is hashcode(K.join_key) % num of reducers

	Grouping phase: 
		- Input: composite_key K1 and K2
		- Output: group K1 and K2 if (K1.join_key == K2.join_key)

	Secondary sort phase:
		- Input: composite_key K1 and K2
		- Output: K1 < K2 (if K1.tag == ref && K2.tag == log)

	Reduce phase:
		- Input: K′: a composite key with the join key and the tag, LIST V': records for K', first from R, then L
		- Buffer records from R (Br) - **only R**
		- Performs a cross-product between records in set of R and L.
		- Output: (null, r*l) with r*l is joined record

**Potential problem:** Both versions (Standard Repartition Join and Improved Repartition Join) include two major sources of overhead that can hurt performance. In particular, both L and R have to be sorted and sent over the network during the shuffle phase of MapReduce

Directed Join
=======
The shuffle over- head in the repartition join can be decreased if both L and R have already been partitioned on the join key before the join operation. This can be accomplished by pre-partitioning L on the join key as log records are generated and by pre- partitioning R on the join key when it is loaded into the DFS. Then at query time, matching partitions from L and R can be directly joined
	
	Inits:
		- If Ri not exist in local storage then remotely retrieve Ri and store locally HRi ← build a hash table from Ri

	Map phase: 
		- Input: (K: null, V : a record from a split of Li)
		- Output: join V with HRi by join key of V and join key of HRi

We can see directed join only store a hash table of Ri (part of R) which is small, so it is avoid run out of my in the case of R is big or L table is skewed. The disadvantage of this approach is that R and L must be pre-partitioning.