## 1. Basic information
- Team #: 18 
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter20-team-18
- Student 1 UCI NetID: siyile
- Student 1 Name: Siyi Le
- Student 2 UCI NetID: jyin12
- Student 2 Name: Jie Yin

## 2. Catalog information about Index
##### Show your catalog information about an index (tables, columns).
We have index table to store index. it has column table-name, attr-name, index, file-name.
We scan the index file at the RM construction function. Write back to memory at RM deconstruction function.

## 3. Block Nested Loop Join (If you have implemented this feature)
##### Describe how your block nested loop join works (especially, how you manage the given buffers.)

1. read tuple from leftIt, till reach the memory limit (block size = (numPages - 2) * PAGE_SIZE, leave two pages for output buffer and right-hand tuple)
2. hash the each block tuple by <key,data> pairs, insert the pairs into hashMap 
3. start table scan right-hand table, scan all the tuples until find a match
4. concatenate left-hand tuple and right-hand tuple into a new tuple, and put it into output buffer
5. If right input reach end, read new left block, and start searching right again.
 
## 4. Index Nested Loop Join (If you have implemented this feature)
##### Describe how your grace hash join works.

1.read a tuple from left-hand side table and get the value of lhsAttr as key
2.find the key in the right-hand side index file and fetch the original tuple
3.concatenate them together to fit in the format of output tuple 
5. If right input reach end, read new left tuple, and start searching right again.

## 5. Grace Hash Join (If you have implemented this feature)
##### Describe how your grace hash join works (especially, in-memory structure).
    
1. allocate both left-hand tuples and right-hand tuples into multiple partitions according to the remainder of key/numPartitions
2. For hashMap, we decide to transfer both TypeInt and TypeReal key into int type. No matter what the value is, same 
value will definitely match to reach other. For Varchar, using the std::hash to get hash. 
3. Do what we do like the BNLJ. If we find a match in corresponding right partition, then concatenate this two tuples together and put the new tuple 
into output buffer.
    
## 6. Aggregation
##### Describe how your aggregation (basic, group-based hash) works.

In basic aggregation, we go through the whole iterator to get aggregations of aggAttr, update these numbers one by one by calling input getNextTuple.

In group-based hash aggregation, we still go through the whole iterator but we use groupAttr data as key, update those groupBy aggregation in hashMaps.
Storing group by results in a vector, and then output them one by one by updating outputIndex of the result vector. 
  
## 7. Implementation Detail
##### Have you added your own source file (.cc or .h)?
NO, we haven't.
##### Have you implemented any optional features? Then, describe them here.
Yes, we implemented Grace Hash Join and Group-based hash aggregation. 

In Grace Hash Join, we firstly scan both left-hand tuples and right-hand tuples and write them into multiple partition files 
according to the remainder of key/numPartitions. For hashMap, we decide to transfer both TypeInt and TypeReal key into int type. No matter what the value is, same 
value will definitely match to reach other.If we find a match in corresponding right partition, then concatenate this two tuples together and put the new tuple 
into output buffer.

In group-based hash aggregation, we still go through the whole iterator but we use groupAttr as key, update those groupBy aggregation in hashMaps.
Storing group by results in a vector, and then output them one by one by updating outputIndex of the result vector. 

##### Other implementation details:


## 6. Other (optional)
##### Freely use this section to tell us about things that are related to the project 4, but not related to the other sections (optional)