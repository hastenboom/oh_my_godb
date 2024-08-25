```text
<START, 1>  // start transaction 1
<COMMIT , 1> // commit transaction 1
<START , 2> //start txn 2
<SETINT, 2, testfile, 1, 80, 1 ,0> // <OP, txnNum, filename, blockNum, offset, value, preVal>
<SETINT, 2, testfile, 1, 80, 2 ,1> // <OP, txnNum, filename, blockNum, offset, value, preVal>
<SETSTRING, 2, testfile, 1, 40, one, one!>
<COMMIT, 2>
<START, 3>
<SETINT, 3, testfile, 1, 80, 2, 9999>
<ROLLBACK, 3>
<START, 4>
<COMMIT, 4>
```
- should be read from down to top
