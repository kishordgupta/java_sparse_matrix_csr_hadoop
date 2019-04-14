# java_sparse_matrix_csr_hadoop
sparse matrix-vector multiplication, namely SpMV, in a parallel computing framework. SpMV is the computation of the following form: ~y = A~x, where A is a sparse matrix, ~y and ~x are dense vectors. \m
\m
Input
Matrix A is stored in the Compressed-Row-Storage (CRS) format, which is represented with three one\m
dimensional arrays that respectively contain the extents of rows, column indices, and nonzero values (see\m
explanation of CRS here). These arrays are stored in three les: \rindx", \cindx", and \data". For example,\m
given a matrix A:\m
A =
2
666664
0 2 0 4 0
0 0 0 0 7
1 0 0 3 0
0 0 0 0 0
0 0 0 2 0
3
777775
\data" will contain one line of space separated numbers:\m
2 4 7 1 3 2
\rindx" will contain one line of space separated numbers:\m
0 2 3 5 5 6
\cindx" will contain one line of space separated numbers:\m
1 3 4 0 3 3
Dense vector ~x is stored in a le named \xvector", which contains one line of space separated numbers. For\m
example:
2 3 5 4 2
Note: You can assume the input matrix is in a shape of nn, however, you do not know the value of n until\m
you nished reading the input. There is a large set of matrices at MatrixMarket available for download.\m
Output
The output of your program should be a le named \yvector", which contains a line of space separated\m
numbers. For example, following the above sample graph and queries, your output le should be\m
22 14 14 0 8
Note: For simplicity, you can assume all numbers are integers.\m
