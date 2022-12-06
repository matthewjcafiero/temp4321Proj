NetIds: yc2478, js2642, mjc487

1. Top level file of the project:
  src/entry/Interpreter.java

2. The logic for index scan operator:
  2.1. the lowkey and highkey:
  In the PhysicalPlanBuilder, we initialized the a new IndexScanOperator object inside the function “visit(LogicalSelectOperator logicalSelectOperator).” The low/highkeys were passed to the IndexScanOperator after we utilized the where expression visitor to get the low/highkeys. The keys are stored in variables “keyL” and “keyH” in the IndexScanOperator class, so IndexReader and getNextTuple could refer to them.
  2.2 Where we handled of clustered vs. unclustered indexes differently:
  In the Catalog class, a variable “indexMap” utilized HashMap<String, String[]> to store a mapping from a table’s name to the table’s index configuration (represented as a String array like “Sailors A 0 10” where “0” represents unclustered).
  Then, the constructor and getNextTuple() function in the IndexScanOperator class utilized this configuration to handle the Un/Clustered indexes separately.
  If Unclustered, the nextRid() function in the IndexReader class and getNextTuple(int[] rid) function in the MyTable class would return the tuples according to the int array rid (rid is the tuple identifier in the form of [pageid, tupleid], so we can directly access the data according to the rid).
  If Clustered, the findRidByKeyL() function in the IndexReader class, and reset(int[] rid) & getNextTupleInBound(int keyH) functions in the MyTable class would return the tuples with indexed column values no larger than the highkey.
  2.3 How we performed the root-to-leaf tree descent and which nodes are deserialized:
  First, the root node of the index tree is read and deserialized, so we can compare the entry key with the lowkey. If the entry key is smaller than the lowkey, then we update the index reader to read and deserialize the node (index page) on the next layer, and continue comparing with the lowkey until we find the index page with the lowkey. Next, we read the leaf nodes with a key no less than the lowkey and no greater than the highkey.

3. The logic in the physical plan builder for separating out the portion of the selection which can/cannot be handled via the index:
  We used a modified version of WhereExprVisitor, which was built to process the where expression of a SQL statement for creating join and select operators.  This visitor already separated out parts of the where expression based on whether they involved 2 columns (implying a join condition), or one column (implying a select condition).  We store each of these in lists based on types.  For use with indexes, we do 2 passes on the list to find which work with indexes.  Our first pass checks if an expression is valid for use with an index based on if its column matches, and if it is valid it calculates whether it would change the upper or lower bound (we start with these values as null).  The second pass gets all conditions that don't involve the index, and returns a new expression putting them all together into one large expression to be used with the select operator.

4. Known bugs: none.
