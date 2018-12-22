package main.dbops;

import java.util.*;

import main.parsetree.TreeNode;
import storageManager.*;


public class DbProcedures {

    private MainMemory mem;
    private Disk disk;
    private SchemaManager schemaManager;
    private HashMap<String, Integer> relationTrack;
    private int relationOffset;
    private HashMap<String, Integer> lastDiskBlockNumber;
    private HashMap<String, Integer> numTuplesInserted;
    private Queue<Integer> emptyMemBlocks;
    private int diskIO;
    private HashMap<String, String> holder;
    private HashMap<String, String> holder2;
    private ArrayList<String> fnames;

    public void getFNames(ArrayList<String> names){
        for(String str: fnames){
            names.add(str);
        }
    }

    public void reset(){
        this.diskIO = 0;
    }

    public int getDiskIO(){
        return diskIO;
    }

    public DbProcedures(MainMemory mem, Disk disk, SchemaManager manager) {
        this.mem = mem;
        this.disk = disk;
        this.schemaManager = manager;
        this.relationTrack = new HashMap<>();
        this.relationOffset = 0;
        this.lastDiskBlockNumber = new HashMap<>();
        this.numTuplesInserted = new HashMap<>();

        this.emptyMemBlocks = new LinkedList<>();
        int memSize = mem.getMemorySize();
        for (int i = 0; i < memSize; i++) {
            emptyMemBlocks.add(i);
        }
        holder = new HashMap<>();
        holder2 = new HashMap<>();
    }

    public boolean createTable(String relationName, Schema schema){
        Relation relation = schemaManager.createRelation(relationName, schema);

        if (relation == null) {
            return false;
        }

        relationTrack.put(relationName, relationOffset);
        relationOffset++;
        lastDiskBlockNumber.put(relationName, -1);
        numTuplesInserted.put(relationName, 0);
        return true;
    }

    public boolean dropTable(String relationName) {
        if (schemaManager.deleteRelation(relationName)) {
            numTuplesInserted.remove(relationName);
            lastDiskBlockNumber.remove(relationName);
            relationTrack.remove(relationName);
            return true;
        }
        return false;
    }

    public boolean insertTuple(String relationName, Tuple t) {

        Relation relation = schemaManager.getRelation(relationName);

        if (relation == null) {
            System.out.println("Relation with the given name not found.");
            return false;
        }

        int numInserted = numTuplesInserted.get(relationName);
        int numPerBlock = t.getTuplesPerBlock();

        if (emptyMemBlocks.size() == 0) {
            System.out.println("Memory full.");
            return false;
        }

        int lastDiskBlock = -1;
        int memBlock = emptyMemBlocks.remove();
        if (numInserted % numPerBlock == 0) {
            lastDiskBlockNumber.put(relationName, lastDiskBlockNumber.get(relationName) + 1);
            lastDiskBlock = lastDiskBlockNumber.get(relationName);
        } else {
            lastDiskBlock = lastDiskBlockNumber.get(relationName);
            if (!relation.getBlock(lastDiskBlock, memBlock)) {
                emptyMemBlocks.add(memBlock);
                return false;
            }
        }

        // at this stage, the disk block is in memory. it also has enough space to accommodate the tuple
        Block block = mem.getBlock(memBlock);

        if (!block.appendTuple(t)) {
            System.out.println("Unable to append tuple to block.");
            emptyMemBlocks.add(memBlock);
            block.clear();
            return false;
        }

        if (!relation.setBlock(lastDiskBlock, memBlock)) {
            emptyMemBlocks.add(memBlock);
            block.clear();
            return false;
        }

        // at this stage, the block has been set back into disk
        emptyMemBlocks.add(memBlock);
        block.clear();
        numTuplesInserted.put(relationName, numTuplesInserted.get(relationName) + 1);
        diskIO++;
        return true;
    }

    public void addBlockToDisk(String relationName, int memBlockId){
        int nextBlock = lastDiskBlockNumber.get(relationName) + 1;
        schemaManager.getRelation(relationName).setBlock(nextBlock, memBlockId);
        lastDiskBlockNumber.put(relationName, nextBlock);
        diskIO++;
    }


    // fetch the last disk block from disk into memory
    // append to this block in memory
    // push this block back to disk
    public boolean insertTuple(String relationName, List<String> fieldNames, List<Object> fieldValues) {
        if (fieldNames.size() != fieldValues.size()) {
            System.out.println("Incorrect input to insertTuple");
            return false;
        }

        Relation relation = schemaManager.getRelation(relationName);

        if (relation == null) {
            System.out.println("Relation with the given name not found.");
            return false;
        }

        Tuple t = relation.createTuple();
        if (!fillTuple(fieldNames, fieldValues, t)) {
            return false;
        }

        int numInserted = numTuplesInserted.get(relationName);
        int numPerBlock = t.getTuplesPerBlock();

        if (emptyMemBlocks.size() == 0) {
            System.out.println("Memory full.");
            return false;
        }

        int lastDiskBlock = -1;
        int memBlock = emptyMemBlocks.remove();
        if (numInserted % numPerBlock == 0) {
            lastDiskBlockNumber.put(relationName, lastDiskBlockNumber.get(relationName) + 1);
            lastDiskBlock = lastDiskBlockNumber.get(relationName);
        } else {
            lastDiskBlock = lastDiskBlockNumber.get(relationName);
            if (!relation.getBlock(lastDiskBlock, memBlock)) {
                emptyMemBlocks.add(memBlock);
                return false;
            }
        }

        // at this stage, the disk block is in memory. it also has enough space to accommodate the tuple
        Block block = mem.getBlock(memBlock);

        if (!block.appendTuple(t)) {
            System.out.println("Unable to append tuple to block.");
            emptyMemBlocks.add(memBlock);
            block.clear();
            return false;
        }

        if (!relation.setBlock(lastDiskBlock, memBlock)) {
            emptyMemBlocks.add(memBlock);
            block.clear();
            return false;
        }

        // at this stage, the block has been set back into disk
        emptyMemBlocks.add(memBlock);
        block.clear();
        numTuplesInserted.put(relationName, numTuplesInserted.get(relationName) + 1);
        diskIO++;
        return true;
    }

    public List<Tuple> selectAllFromTable(String relationName) {

        List<Tuple> tuples = new ArrayList<>();
        if (emptyMemBlocks.size() == 0) {
            System.out.println("Memory full.");
            return null;
        }

        Relation relation = schemaManager.getRelation(relationName);

        if (relation == null) {
            System.out.println("Relation with the given name not found.");
            return null;
        }

        int lastBlock = lastDiskBlockNumber.get(relationName);
        for (int i = 0; i <= lastBlock; i++) {
            int memBlock = emptyMemBlocks.remove();
            if (!relation.getBlock(i, memBlock)) {
                print("Unable to copy block to memory.");
                emptyMemBlocks.add(memBlock);
                return null;
            }
            diskIO++;
            Block block = mem.getBlock(memBlock);
            for (Tuple tuple : block.getTuples()) {
                if(!tuple.isNull()){
                    tuples.add(tuple);
                }
            }
            block.clear();
            emptyMemBlocks.add(memBlock);
        }
        return tuples;
    }

    public boolean deleteFromTable(String relationName, TreeNode searchCondition){
        List<Tuple> tuples = new ArrayList<>();
        if (emptyMemBlocks.size() == 0) {
            System.out.println("Memory full.");
            return false;
        }

        Relation relation = schemaManager.getRelation(relationName);

        if (relation == null) {
            System.out.println("Relation with the given name not found.");
            return false;
        }

        int lastBlock = lastDiskBlockNumber.get(relationName);
        for (int i = 0; i <= lastBlock; i++) {
            int memBlock = emptyMemBlocks.remove();
            if (!relation.getBlock(i, memBlock)) {
                print("Unable to copy block to memory.");
                emptyMemBlocks.add(memBlock);
                return false;
            }
            diskIO++;
            Block block = mem.getBlock(memBlock);
            if (emptyMemBlocks.size() == 0) {
                System.out.println("Memory full.");
                return false;
            }

            int newMem = emptyMemBlocks.remove();
            Block newBlock = mem.getBlock(newMem);
            for (Tuple tuple : block.getTuples()) {
                List<Tuple> table = new ArrayList<>();
                table.add(tuple);
                // print(tuple.toString());

                SearchCondEvaluator evaluator = new SearchCondEvaluator();
                List<Tuple> solution = table;
                if(searchCondition != null){
                    solution = evaluator.evaluate(table, searchCondition);
                }

                // System.out.println("Soln size: " + solution.size());
                if(solution.size() == 1){
                    tuple.invalidate();
                }
                newBlock.appendTuple(tuple);
            }

            if (!relation.setBlock(i, newMem)) {
                block.clear();
                newBlock.clear();
                emptyMemBlocks.add(memBlock);
                emptyMemBlocks.add(newMem);
                return false;
            }
            diskIO++;
            block.clear();
            newBlock.clear();
            emptyMemBlocks.add(memBlock);
            emptyMemBlocks.add(newMem);
        }

        return true;
    }

    public List<Tuple> tablesCrossProduct(List<String> relationNames){
        if(relationNames.size() == 1){
            return selectAllFromTable(relationNames.get(0));
        }
        ArrayList<String> columns = new ArrayList<>();
        ArrayList<FieldType> allTypes = new ArrayList<>();
        List<Integer> offsets = new ArrayList<>();

        int i=0;
        int size = 1;
        for(String relationName: relationNames){
            if(relationTrack.get(relationName) == null){
                print("Relation " + relationName + " does not exist.");
                return null;
            }
            Schema schema = schemaManager.getSchema(relationName);
            List<String> names = schema.getFieldNames();
            List<FieldType> types = schema.getFieldTypes();

            for(String name: names){
                columns.add(relationName + "." + name);
            }
            for(FieldType type: types){
                allTypes.add(type);
            }

            for(int j=0; j < names.size(); j++){
                offsets.add(i); i++;
            }
            size *= schemaManager.getRelation(relationName).getNumOfTuples();
        }
        List<List<Object>> allFieldValues = new ArrayList<>();
        allFieldValues.add(new ArrayList<>());

        for(String relationName: relationNames){
            List<Tuple> tuples = selectAllFromTable(relationName);
            List<List<Object>> objects = new ArrayList<>();

            Schema schema = schemaManager.getRelation(relationName).getSchema();
            List<String> fieldNames = schema.getFieldNames();
            List<FieldType> fieldTypes = schema.getFieldTypes();
            for(Tuple tuple: tuples){
                List<Object> obj = new ArrayList<>();
                for(int p=0; p < fieldNames.size(); p++){
                    String val = tuple.getField(fieldNames.get(p)).toString();


                    if(fieldTypes.get(p).equals(FieldType.STR20)){
                        obj.add(val);
                    }else if(fieldTypes.get(p).equals(FieldType.INT)){
                        if(Integer.parseInt(val) == Integer.MIN_VALUE){
                            obj.add(null);
                        }else{
                            obj.add(Integer.parseInt(val));
                        }
                    }else{
                        print("Some error found for field type."); return null;
                    }
                }
                objects.add(obj);
            }
            allFieldValues = crossProduct(allFieldValues, objects);
        }

        // printCrossProduct(columns, allFieldValues);

        Schema newSchema = new Schema(columns, allTypes);
        String relationName = Integer.toString(relationOffset+1) + "-tempRel";
        Relation relation = schemaManager.createRelation(relationName, newSchema);

        if(relation == null){
            print("Error creating temp relation.");
            return null;
        }

        relationTrack.put(relationName, relationOffset);
        relationOffset++;
        lastDiskBlockNumber.put(relationName, -1);
        numTuplesInserted.put(relationName, 0);

        List<Tuple> result = new ArrayList<>();
        for(int k=0; k < allFieldValues.size(); k++){
            Tuple t = relation.createTuple();
            if(!fillTuple(columns, allFieldValues.get(k), t)){
                // schemaManager.deleteRelation(relationName);
                print("Couldn't fill tuple."); return null;
            }
            result.add(t);
        }
        // schemaManager.deleteRelation(relationName);
        return result;
    }

    public boolean insertAllFromAToB(String A, String B){
        if (emptyMemBlocks.size() == 0) {
            System.out.println("Memory full while inserting all.");
            return false;
        }

        Relation relation = schemaManager.getRelation(A);

        if (relation == null) {
            System.out.println("Relation with the given name not found.");
            return false;
        }

        int lastBlock = lastDiskBlockNumber.get(A);
        for (int i = 0; i <= lastBlock; i++) {
            int memBlock = emptyMemBlocks.remove();
            if (!relation.getBlock(i, memBlock)) {
                print("Unable to copy block to memory.");
                emptyMemBlocks.add(memBlock);
                return false;
            }
            diskIO++;
            Block block = mem.getBlock(memBlock);
            for (Tuple tuple : block.getTuples()) {
                if(!insertTuple(B, tuple)){
                    print("Unable to insert.");
                    block.clear();
                    emptyMemBlocks.add(memBlock);
                    return false;
                }
            }
            block.clear();
            emptyMemBlocks.add(memBlock);
        }
        return true;
    }

    // Execution algorithms

    //1. One pass projection
    // Do we have access to colTypes
    public String onePassProjection(String relationName, List<String> cols,
                                                boolean getOutput, List<Tuple> output, boolean isAll){

        ArrayList<String> columns = new ArrayList<>(cols);
        if(isAll){
            List<String> allCols = schemaManager.getRelation(relationName).getSchema().getFieldNames();
            for(String c: allCols){
                columns.add(c);
                cols.add(c);
            }
        }
        if(emptyMemBlocks.size() == 0){
            System.out.println("Memory full.");
            return "";
        }

        Relation relation = schemaManager.getRelation(relationName);

        if(relation == null){
            System.out.println("Relation with the given name not found.");
            return "";
        }

        ArrayList<FieldType> types = new ArrayList<>();
        for(String col: columns){
            types.add(relation.getSchema().getFieldType(col));
        }
        Schema newSchema = new Schema(columns, types);
        String tempRel = "tempRelation" + Integer.toString(relationOffset + 1);
        createTable(tempRel, newSchema);

        Relation newRel = schemaManager.getRelation(tempRel);
        fnames = newRel.getSchema().getFieldNames();
        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);

        int lastBlock = lastDiskBlockNumber.get(relationName);

        for(int i=0; i <= lastBlock; i++){
            int memBlock = emptyMemBlocks.remove();
            if(!relation.getBlock(i, memBlock)){
                print("Unable to copy block to memory.");
                emptyMemBlocks.add(memBlock);
                return "";
            }
            diskIO++;
            Block block = mem.getBlock(memBlock);
            for(Tuple tuple: block.getTuples()){
                if(tuple.isNull()){
                    continue;
                }
                Tuple newTuple = newRel.createTuple();
                for(int j=0; j < columns.size(); j++){
                    String col = columns.get(j);

                    try {
                        if(tuple.getField(col).type == FieldType.INT){
                            newTuple.setField(col, Integer.parseInt(tuple.getField(col).toString()));
                        }else if(tuple.getField(col).type == FieldType.STR20){
                            newTuple.setField(col, tuple.getField(col).toString());
                        }

                    }catch (Exception e){
                        print("Exception caught.");
                        block.clear();
                        newBlock.clear();
                        emptyMemBlocks.add(newMemBlock);
                        emptyMemBlocks.add(memBlock);
                        return "";
                    }
                }
                if(!getOutput){
                    if(newBlock.isFull()){
                        addBlockToDisk(tempRel, newMemBlock);
                        newBlock.clear();
                        diskIO++;
                    }
                    newBlock.appendTuple(newTuple);
                }else{
                    output.add(newTuple);
                }
            }
            block.clear();
            emptyMemBlocks.add(memBlock);
        }
        addBlockToDisk(tempRel, newMemBlock);
        diskIO++;
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);

        if(getOutput){
            return "";
        }
        return tempRel;
    }

    //2. One pass selection
    // always writes temporary relation to disk
    public String onePassSelection(String relationName, TreeNode searchCondition){
        if(emptyMemBlocks.size() == 0){
            System.out.println("Memory full.");
            return null;
        }

        Relation relation = schemaManager.getRelation(relationName);
        String tempRel = "selRel" + Integer.toString(relationOffset + 1);
        holder.put(tempRel, relationName);
        holder2.put(relationName, tempRel);
        createTable(tempRel, relation.getSchema());
        Relation newRel = schemaManager.getRelation(tempRel);
        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);

        if(relation == null){
            System.out.println("Relation with the given name not found.");
            return null;
        }

        int lastBlock = lastDiskBlockNumber.get(relationName);
        for(int i=0; i <= lastBlock; i++){
            int memBlock = emptyMemBlocks.remove();
            if(!relation.getBlock(i, memBlock)){
                print("Unable to copy block to memory.");
                emptyMemBlocks.add(memBlock);
                return null;
            }
            diskIO++;
            Block block = mem.getBlock(memBlock);
            List<Tuple> tuples = block.getTuples();
            SearchCondEvaluator evaluator = new SearchCondEvaluator();
            List<Tuple> tempRes = evaluator.evaluate(tuples, searchCondition);

            for(Tuple t: tempRes){
                if(t.isNull()){
                    continue;
                }
                if(newBlock.isFull()){
                    addBlockToDisk(tempRel, newMemBlock);
                    newBlock.clear();
                }
                newBlock.appendTuple(t);
            }
            block.clear();
            emptyMemBlocks.add(memBlock);
        }
        addBlockToDisk(tempRel, newMemBlock);
        diskIO++;
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);
        return tempRel;
    }

    //3. One pass duplicate elimination
    public String onePassDupElimination(String relationName, boolean getOutput, List<Tuple> res){

        if(emptyMemBlocks.size() == 0){
            System.out.println("Memory full.");
            return null;
        }

        int totalNumTuplesInRelation = schemaManager.getRelation(relationName).getNumOfTuples();
        int numTuplesPerBlock = schemaManager.getRelation(relationName).getSchema().getTuplesPerBlock();
        int numAvailableBlocks = emptyMemBlocks.size()-1;

        if(numAvailableBlocks*numTuplesPerBlock < totalNumTuplesInRelation){
            // two pass duplicate elimination called here
            print("Two pass elimination called");
            return twoPassDuplicateElimination(relationName, getOutput, res);
        }

        Relation relation = schemaManager.getRelation(relationName);

        if(relation == null){
            System.out.println("Relation with the given name not found.");
            return null;
        }
        int lastBlock = lastDiskBlockNumber.get(relationName);

        List<Integer> memBlocks = new ArrayList<>();
        for(int i=0; i <= lastBlock; i++){
            if(emptyMemBlocks.size() == 0){
                System.out.println("Memory full.");
                return null;
            }
            int memBlock = emptyMemBlocks.remove();
            memBlocks.add(memBlock);
            if(!relation.getBlock(i, memBlock)){
                print("Unable to copy block to memory.");
                for(Integer m: memBlocks){
                    emptyMemBlocks.add(m);
                }
                return null;
            }
            diskIO++;
            Block block = mem.getBlock(memBlock);
            ArrayList<Tuple> tuples = block.getTuples();
            Collections.sort(tuples, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    return o1.toString().compareTo(o2.toString());
                }
            });

            block.setTuples(tuples);
        }

        PriorityQueue<Pair> queue = new PriorityQueue<>(new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                return o1.t.toString().compareTo(o2.t.toString());
            }
        });
        for(int i=0; i < memBlocks.size(); i++){
            Pair p = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(0), 0, i);
            queue.add(p);
        }

        Tuple prev = null;

        String newRelName = "tempRelation" + Integer.toString(relationOffset + 1);
        createTable(newRelName, relation.getSchema());
        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);
        try {
            while(!queue.isEmpty()){
                Pair p = queue.remove();
                Tuple curr = p.t;
                int idx = p.idx;
                int i = p.i;
                if(prev == null){
                    if(getOutput){
                        res.add(curr);
                    }else{
                        if(curr.isNull()){
                            continue;
                        }
                        if(newBlock.isFull()){
                            addBlockToDisk(newRelName, newMemBlock);
                            newBlock.clear();
                            diskIO++;
                        }
                        newBlock.appendTuple(curr);
                    }
                    prev = curr;

                    if(idx + 1 < mem.getBlock(memBlocks.get(i)).getNumTuples()){
                        Pair p1 = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(idx+1), idx+1, i);
                        queue.add(p1);
                    }
                }else if(curr.toString().equals(prev.toString())){
                    prev = curr;

                    if(idx + 1 < mem.getBlock(memBlocks.get(i)).getNumTuples()){
                        Pair p1 = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(idx+1), idx+1, i);
                        queue.add(p1);
                    }
                }else{
                    if(getOutput){
                        if(curr.isNull()){
                            continue;
                        }
                        res.add(curr);
                    }else{
                        if(curr.isNull()){
                            continue;
                        }
                        if(newBlock.isFull()){
                            addBlockToDisk(newRelName, newMemBlock);
                            newBlock.clear();
                        }
                        newBlock.appendTuple(curr);
                    }
                    prev = curr;

                    if(idx + 1 < mem.getBlock(memBlocks.get(i)).getNumTuples()){
                        Pair p1 = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(idx+1), idx+1, i);
                        queue.add(p1);
                    }
                }
            }
        }catch (Exception e){
            print("Exception occurred with tuples.");
            for(Integer m: memBlocks){
                emptyMemBlocks.add(m);
            }
            newBlock.clear();
            emptyMemBlocks.add(newMemBlock);
            return null;
        }

        addBlockToDisk(newRelName, newMemBlock);
        diskIO++;
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);
        for(Integer m: memBlocks){
            emptyMemBlocks.add(m);
        }
        return newRelName;
    }

    //3. One pass sorting
    public String onePassSorting(String relationName, boolean getOutput, List<Tuple> res, String colName){

        int totalNumTuplesInRelation = schemaManager.getRelation(relationName).getNumOfTuples();
        int numTuplesPerBlock = schemaManager.getRelation(relationName).getSchema().getTuplesPerBlock();
        int numAvailableBlocks = emptyMemBlocks.size()-1;

        if(numAvailableBlocks*numTuplesPerBlock < totalNumTuplesInRelation){
            // two pass sorting called here
            print("Two pass sorting called");
            return twoPassSorting(relationName, getOutput, res, colName);
        }

        if(emptyMemBlocks.size() == 0){
            System.out.println("Memory full.");
            return null;
        }

        Relation relation = schemaManager.getRelation(relationName);

        if(relation == null){
            System.out.println("Relation with the given name not found.");
            return null;
        }
        int lastBlock = lastDiskBlockNumber.get(relationName);

        List<Integer> memBlocks = new ArrayList<>();
        for(int i=0; i <= lastBlock; i++){
            if(emptyMemBlocks.size() == 0){
                System.out.println("Memory full.");
                return null;
            }
            int memBlock = emptyMemBlocks.remove();
            memBlocks.add(memBlock);
            if(!relation.getBlock(i, memBlock)){
                print("Unable to copy block to memory.");
                for(Integer m: memBlocks){
                    emptyMemBlocks.add(m);
                }
                diskIO++;
                return null;
            }
            Block block = mem.getBlock(memBlock);
            ArrayList<Tuple> tuples = block.getTuples();
            Collections.sort(tuples, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    if(o1.getField(colName).type == FieldType.STR20){
                        return o1.getField(colName).toString().compareTo(o2.getField(colName).toString());
                    }else{
                        return Integer.parseInt(o1.getField(colName).toString()) - Integer.parseInt(o2.getField(colName).toString());
                    }
                }
            });

            block.setTuples(tuples);
        }

        PriorityQueue<Pair> queue = new PriorityQueue<>(new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                if(o1.t.getField(colName).type == FieldType.STR20){
                    return o1.t.getField(colName).toString().compareTo(o2.t.getField(colName).toString());
                }else{
                    return Integer.parseInt(o1.t.getField(colName).toString()) - Integer.parseInt(o2.t.getField(colName).toString());
                }

            }
        });
        for(int i=0; i < memBlocks.size(); i++){
            Pair p = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(0), 0, i);
            queue.add(p);
        }

        String newRelName = "tempRelation" + Integer.toString(relationOffset + 1);
        createTable(newRelName, relation.getSchema());
        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);

        try {
            while(!queue.isEmpty()){
                Pair p = queue.remove();
                Tuple curr = p.t;
                int idx = p.idx;
                int i = p.i;

                if(getOutput){
                    if(curr.isNull()){
                        continue;
                    }
                    res.add(curr);
                }else{
                    if(curr.isNull()){
                        continue;
                    }
                    if(newBlock.isFull()){
                        addBlockToDisk(newRelName, newMemBlock);
                        newBlock.clear();
                        diskIO++;
                    }
                    newBlock.appendTuple(curr);
                }
                if(idx + 1 < mem.getBlock(memBlocks.get(i)).getNumTuples()){
                    Pair p1 = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(idx+1), idx+1, i);
                    queue.add(p1);
                }
            }
        }catch (Exception e){
            print("Exception occurred with tuples.");
            for(Integer m: memBlocks){
                emptyMemBlocks.add(m);
                mem.getBlock(m).clear();
            }
            newBlock.clear();
            emptyMemBlocks.add(newMemBlock);
            return null;
        }
        addBlockToDisk(newRelName, newMemBlock);
        diskIO++;
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);
        for(Integer m: memBlocks){
            emptyMemBlocks.add(m);
            mem.getBlock(m).clear();
        }
        return newRelName;
    }

    public String twoPassDuplicateElimination(String relationName, boolean getOutput, List<Tuple> result){
        firstPass(relationName, null);
        int size = emptyMemBlocks.size()-1;
        int numBlocks = lastDiskBlockNumber.get(relationName) + 1;

        Relation relation = schemaManager.getRelation(relationName);
        int start = 0;

        List<Integer> memBlocks = new ArrayList<>();
        List<Integer> bN = new ArrayList<>();
        List<Integer> eB = new ArrayList<>();
        for(int i= start; i < numBlocks; i+=size){
            int memBlock = emptyMemBlocks.remove();
            relation.getBlock(i, memBlock);
            memBlocks.add(memBlock);
            bN.add(i);
            diskIO++;
            eB.add((int)Math.min(i+size-1, numBlocks-1));
        }

        PriorityQueue<Triplet> queue = new PriorityQueue<>(new Comparator<Triplet>() {
            @Override
            public int compare(Triplet o1, Triplet o2) {
                return o1.tuple.toString().compareTo(o2.tuple.toString());
            }
        });
        for(int i=0; i < memBlocks.size(); i++){
            queue.add(new Triplet(mem.getBlock(memBlocks.get(i)).getTuple(0), bN.get(i), 0, eB.get(i), memBlocks.get(i)));
        }

        String newRelName = "tempRelation" + Integer.toString(relationOffset + 1);
        createTable(newRelName, relation.getSchema());
        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);

        Tuple prev = null;
        while(!queue.isEmpty()){
            Triplet curr = queue.remove();
            addNextTripletToQueue(queue, curr, relation.getSchema(), relationName);

            if(prev != null && prev.toString().equals(curr.tuple.toString())){
                prev = curr.tuple;
                continue;
            }

            if(getOutput){
                result.add(curr.tuple);
            }else{
                if(curr.tuple.isNull()){
                    continue;
                }
                if(newBlock.isFull()){
                    addBlockToDisk(newRelName, newMemBlock);
                    newBlock.clear();
                }
                newBlock.appendTuple(curr.tuple);
            }
            prev = curr.tuple;
        }
        addBlockToDisk(newRelName, newMemBlock);
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);
        for(Integer m: memBlocks){
            emptyMemBlocks.add(m);
            mem.getBlock(m).clear();
        }
        return newRelName;
    }

    public String twoPassSorting(String relationName, boolean getOutput, List<Tuple> result, String colName){
        firstPass(relationName, colName);
        int size = emptyMemBlocks.size()-1;
        int numBlocks = lastDiskBlockNumber.get(relationName) + 1;

        Relation relation = schemaManager.getRelation(relationName);
        int start = 0;

        List<Integer> memBlocks = new ArrayList<>();
        List<Integer> bN = new ArrayList<>();
        List<Integer> eB = new ArrayList<>();
        for(int i= start; i < numBlocks; i+=size){
            int memBlock = emptyMemBlocks.remove();
            relation.getBlock(i, memBlock);
            memBlocks.add(memBlock);
            bN.add(i);
            eB.add((int)Math.min(i+size-1, numBlocks-1));
        }

        PriorityQueue<Triplet> queue = new PriorityQueue<>(new Comparator<Triplet>() {
            @Override
            public int compare(Triplet o1, Triplet o2) {
                if(o1.tuple.getField(colName).type == FieldType.STR20){
                    return o1.tuple.getField(colName).toString().compareTo(o2.tuple.getField(colName).toString());
                }else{
                    return Integer.parseInt(o1.tuple.getField(colName).toString()) - Integer.parseInt(o2.tuple.getField(colName).toString());
                }
            }
        });
        for(int i=0; i < memBlocks.size(); i++){
            queue.add(new Triplet(mem.getBlock(memBlocks.get(i)).getTuple(0), bN.get(i), 0, eB.get(i), memBlocks.get(i)));
        }

        String newRelName = "tempRelation" + Integer.toString(relationOffset + 1);
        createTable(newRelName, relation.getSchema());
        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);


        while(!queue.isEmpty()){
            Triplet curr = queue.remove();
            addNextTripletToQueue(queue, curr, relation.getSchema(), relationName);

            if(getOutput){
                result.add(curr.tuple);
            }else{
                if(curr.tuple.isNull()){
                    continue;
                }
                if(newBlock.isFull()){
                    addBlockToDisk(newRelName, newMemBlock);
                    newBlock.clear();
                    diskIO++;
                }
                newBlock.appendTuple(curr.tuple);
            }
        }
        addBlockToDisk(newRelName, newMemBlock);
        diskIO++;
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);
        for(Integer m: memBlocks){
            emptyMemBlocks.add(m);
            mem.getBlock(m).clear();
        }
        return newRelName;
    }

    public String twoPassJoin(String relationName1, String relationName2, TreeNode searchCondition, String colName1, String colName2){
        if(colName1 == null || colName2 == null){
            String ret = twoPassJoinImpl(relationName1, relationName2, searchCondition);
            return  ret;
        }
        return null;
    }

    public int getSize(String relationName){
        return schemaManager.getRelation(relationName).getNumOfTuples();
    }

    private String twoPassJoinImpl(String relationName1, String relationName2, TreeNode searchCondition){
        int size1 = lastDiskBlockNumber.get(relationName1) + 1;
        int size2 = lastDiskBlockNumber.get(relationName2) + 1;

        if(size1 > size2){
            String temp = relationName2;
            relationName2  = relationName1;
            relationName1 = temp;
        }

        int size = emptyMemBlocks.size()-2;

        Relation relation1 = schemaManager.getRelation(relationName1);
        Relation relation2 = schemaManager.getRelation(relationName2);
        String newRelName = "tempRel" + Integer.toString(relationOffset + 1);

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<String> fn1 = relation1.getSchema().getFieldNames();
        ArrayList<String> fn2 = relation2.getSchema().getFieldNames();

        for(String f: fn1){

            if(holder.get(relationName1) != null){
                fieldNames.add(holder.get(relationName1) + "." + f);
                continue;
            }

            if(!relationName1.contains("temp")){
                fieldNames.add(relationName1 + "." + f);
            }else{
                fieldNames.add(f);
            }
        }
        for(String f: fn2){

            if(holder.get(relationName2) != null){
                fieldNames.add(holder.get(relationName2) + "." + f);
                continue;
            }

            if(!relationName2.contains("temp")){
                fieldNames.add(relationName2 + "." + f);
            }else{
                fieldNames.add(f);
            }
        }

        ArrayList<FieldType> fieldTypes = new ArrayList<>();
        ArrayList<FieldType> ft1 = relation1.getSchema().getFieldTypes();
        ArrayList<FieldType> ft2 = relation2.getSchema().getFieldTypes();

        for(FieldType f: ft1){
            fieldTypes.add(f);
        }
        for(FieldType f: ft2){
            fieldTypes.add(f);
        }
        Schema schema = new Schema(fieldNames, fieldTypes);
        createTable(newRelName, schema);

        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);

        int end = 0;
        while(true){
            int start = end;
            if(start > lastDiskBlockNumber.get(relationName1)){
                break;
            }
            end = (int)Math.min(start+size, lastDiskBlockNumber.get(relationName1) + 1);

            List<Integer> memBlocks = new ArrayList<>();
            for(int i=start; i < end; i++){
                int memBlock = emptyMemBlocks.remove();
                memBlocks.add(memBlock);
                relation1.getBlock(i, memBlock);
                diskIO++;
            }
            for(int i=0; i <= lastDiskBlockNumber.get(relationName2); i++){
                int secondMem = emptyMemBlocks.remove();
                relation2.getBlock(i, secondMem);
                diskIO++;
                Block secBlock = mem.getBlock(secondMem);

                for(Tuple secTuple: secBlock.getTuples()){
                    for(int j: memBlocks){
                        Block firstBlock = mem.getBlock(j);
                        for(Tuple firstTuple: firstBlock.getTuples()){
                            Tuple t = schemaManager.getRelation(newRelName).createTuple();
                            joinTuples(firstTuple, secTuple, t, relationName1, relationName2);

                            List<Tuple> oneTuple = new ArrayList<>();
                            oneTuple.add(t);
                            SearchCondEvaluator evaluator = new SearchCondEvaluator();
                            List<Tuple> result = evaluator.evaluate(oneTuple, searchCondition);

                            if(result.size() == 0){
                                continue;
                            }
                            if(newBlock.isFull()){
                                addBlockToDisk(newRelName, newMemBlock);
                                diskIO++;
                                newBlock.clear();
                            }
                            newBlock.appendTuple(t);
                        }
                    }
                }
                emptyMemBlocks.add(secondMem);
                secBlock.clear();
            }
            for(int i: memBlocks){
                mem.getBlock(i).clear();
                emptyMemBlocks.add(i);
            }
        }
        addBlockToDisk(newRelName, newMemBlock);
        diskIO++;
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);

        return newRelName;
    }

    private void joinTuples(Tuple t1, Tuple t2, Tuple t, String rel1, String rel2){
        ArrayList<String> fieldNames = t1.getSchema().getFieldNames();
        ArrayList<FieldType> types = t1.getSchema().getFieldTypes();

        for(int i=0; i < fieldNames.size(); i++){
            String fn = (rel1.contains("temp")) ? fieldNames.get(i) : rel1 + "." + fieldNames.get(i);
            if(fn.contains("selRel")){
                fn = holder.get(fn.split("\\.")[0]) + "." + fn.split("\\.")[1];
            }
            String fn2 = fieldNames.get(i);
            if(fieldNames.get(i).contains("\\.")){
                fn2 = holder2.get(rel1) != null ? holder2.get(rel1) + "." + fieldNames.get(i).split("\\.")[1] : fieldNames.get(i);
            }
            if(types.get(i).equals(FieldType.STR20)){
                t.setField(fn, t1.getField(fn2).toString());
            }else if(types.get(i).equals(FieldType.INT)){
                t.setField(fn, Integer.parseInt(t1.getField(fn2).toString()));
            }
        }

        fieldNames = t2.getSchema().getFieldNames();
        types = t2.getSchema().getFieldTypes();

        for(int i=0; i < fieldNames.size(); i++){
            String fn = (rel2.contains("temp")) ? fieldNames.get(i) : rel2 + "." + fieldNames.get(i);
            if(fn.contains("selRel")){
                fn = holder.get(fn.split("\\.")[0]) + "." + fn.split("\\.")[1];
            }
            String fn2 = fieldNames.get(i);
            if(fieldNames.get(i).contains("\\.")){
                fn2 = holder2.get(rel2) != null ? holder2.get(rel2) + "." + fieldNames.get(i).split("\\.")[1] : fieldNames.get(i);
            }
            if(types.get(i).equals(FieldType.STR20)){
                t.setField(fn, t2.getField(fn2).toString());
            }else if(types.get(i).equals(FieldType.INT)){
                t.setField(fn, Integer.parseInt(t2.getField(fn2).toString()));
            }
        }
    }

    private void addNextTripletToQueue(PriorityQueue<Triplet> queue, Triplet curr, Schema schema, String relationName){
        if(curr.tupleOffset+1 < schema.getTuplesPerBlock()){
            if(mem.getBlock(curr.memBlock).getNumTuples() <= curr.tupleOffset+1){
                return;
            }
            Tuple t1 = mem.getBlock(curr.memBlock).getTuple(curr.tupleOffset + 1);
            queue.add(new Triplet(t1, curr.blockNumber, curr.tupleOffset + 1, curr.endBlock, curr.memBlock));
            return;
        }
        int bn = curr.blockNumber + 1;
        if(bn <= curr.endBlock){
            mem.getBlock(curr.memBlock).clear();
            schemaManager.getRelation(relationName).getBlock(bn, curr.memBlock);
            Tuple t1 = mem.getBlock(curr.memBlock).getTuple(0);
            queue.add(new Triplet(t1, curr.blockNumber+ 1, 0, curr.endBlock, curr.memBlock));
            diskIO++;
        }
    }

    private void firstPass(String relationName, String colName){

        int numBlocks = lastDiskBlockNumber.get(relationName) + 1;
        int memBlocksAvailable = emptyMemBlocks.size()-1;

        Relation relation = schemaManager.getRelation(relationName);
        int end = 0;


        String newRelName = relationName;
        int newMemBlock = emptyMemBlocks.remove();
        Block newBlock = mem.getBlock(newMemBlock);
        int k = 0;

        // Sorting and putting back into disk
        while(true){
            int start = end;
            if(start >= numBlocks){
                break;
            }
            end = (int)Math.min(start + memBlocksAvailable, numBlocks);
            List<Integer> memBlocks = new ArrayList<>();
            for(int i=start; i < end; i++){
                int memBlock = emptyMemBlocks.remove();
                memBlocks.add(memBlock);
                if(!relation.getBlock(i, memBlock)){
                    print("Unable to copy block to memory.");
                    for(Integer m: memBlocks){
                        emptyMemBlocks.add(m);
                    }
                    diskIO++;
                    return ;
                }
                Block block = mem.getBlock(memBlock);
                ArrayList<Tuple> tuples = block.getTuples();
                Collections.sort(tuples, new Comparator<Tuple>() {
                    @Override
                    public int compare(Tuple o1, Tuple o2) {
                        // required for duplicate elimination
                        if(colName == null){
                            return o1.toString().compareTo(o2.toString());
                        }
                        if(o1.getField(colName).type == FieldType.STR20){
                            return o1.getField(colName).toString().compareTo(o2.getField(colName).toString());
                        }else{
                            return Integer.parseInt(o1.getField(colName).toString()) - Integer.parseInt(o2.getField(colName).toString());
                        }
                    }
                });

                block.setTuples(tuples);
            }

            PriorityQueue<Pair> queue = new PriorityQueue<>(new Comparator<Pair>() {
                @Override
                public int compare(Pair o1, Pair o2) {
                    // required for duplicate elimination
                    if(colName == null){
                        return o1.t.toString().compareTo(o2.t.toString());
                    }

                    if(o1.t.getField(colName).type == FieldType.STR20){
                        return o1.t.getField(colName).toString().compareTo(o2.t.getField(colName).toString());
                    }else{
                        return Integer.parseInt(o1.t.getField(colName).toString()) - Integer.parseInt(o2.t.getField(colName).toString());
                    }

                }
            });
            for(int i=0; i < memBlocks.size(); i++){
                Pair p = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(0), 0, i);
                queue.add(p);
            }

            try {
                while(!queue.isEmpty()){
                    Pair p = queue.remove();
                    Tuple curr = p.t;
                    int idx = p.idx;
                    int i = p.i;

                    if(curr.isNull()){
                        continue;
                    }
                    if(newBlock.isFull()){
                        relation.setBlock(k, newMemBlock);
                        newBlock.clear();
                        diskIO++;
                        k++;
                    }
                    newBlock.appendTuple(curr);

                    if(idx + 1 < mem.getBlock(memBlocks.get(i)).getNumTuples()){
                        Pair p1 = new Pair(mem.getBlock(memBlocks.get(i)).getTuple(idx+1), idx+1, i);
                        queue.add(p1);
                    }
                }
            }catch (Exception e){
                print("Exception occurred with tuples.");
                for(Integer m: memBlocks){
                    emptyMemBlocks.add(m);
                    mem.getBlock(m).clear();
                }
                newBlock.clear();
                emptyMemBlocks.add(newMemBlock);
                return;
            }

            for(Integer m: memBlocks){
                emptyMemBlocks.add(m);
                mem.getBlock(m).clear();
            }
        }
        relation.setBlock(k, newMemBlock);
        diskIO++;
        k++;
        newBlock.clear();
        emptyMemBlocks.add(newMemBlock);
    }

    private void printCrossProduct(List<String> columns, List<List<Object>> allFieldValues){
        print("-----Printing cross product-------");
        for(int h=0; h < columns.size(); h++){
            System.out.print(columns.get(h) + " ");
        }
        System.out.println();
        System.out.println("-----------------------");
        for(int h=0; h < allFieldValues.size(); h++){
            for(int g = 0; g < allFieldValues.get(h).size(); g++){
                System.out.print(allFieldValues.get(h).get(g) + "  ");
            }
            System.out.println();
        }
        System.out.println();
    }

    private List<List<Object>> crossProduct(List<List<Object>> a, List<List<Object>> b){
        int size1 = a.size();
        int size2 = b.size();

        List<List<Object>> c = new ArrayList<>();
        for(int i=0; i < size1; i++){
            for(int j=0; j < size2; j++){
                List<Object> c1 = new ArrayList<>();
                List<Object> a1 = a.get(i);
                List<Object> b1 = b.get(j);

                for(Object obj: a1){
                    c1.add(obj);
                }
                for(Object obj: b1){
                    c1.add(obj);
                }
                c.add(c1);
            }
        }
        return c;
    }

    private boolean fillTuple(List<String> fieldNames, List<Object> fieldValues, Tuple t) {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldValues.get(i) instanceof Integer) {
                if (!t.setField(fieldNames.get(i), (int) fieldValues.get(i))) {
                    return false;
                }
            } else if (fieldValues.get(i) instanceof String) {
                if (!t.setField(fieldNames.get(i), (String) fieldValues.get(i))) {
                    return false;
                }
            } else if (fieldValues.get(i) == null){
                // t.setField(fieldNames.get(i), null);
            } else {
                System.out.println("Incorrect input. Type not found: ");
                return false;
            }
        }
        return true;
    }

    private boolean populateFieldTypes(List<String> types, List<FieldType> fieldTypes) {
        for (String type : types) {
            if (type.equals("STR20")) {
                fieldTypes.add(FieldType.STR20);
            } else if (type.equals("INT")) {
                fieldTypes.add(FieldType.INT);
            } else {
                return false;
            }
        }
        return true;
    }

    private void print(String s) {
        System.out.println(s);
    }

    private void print(int i) {
        System.out.println(i);
    }
}

class Pair{
    Tuple t;
    int idx;
    int i;

    public Pair(Tuple t, int index, int i){
        this.t = t;
        this.idx = index;
        this.i = i;
    }
}

class Triplet {
    Tuple tuple;
    int blockNumber;
    int tupleOffset;
    int endBlock;
    int memBlock;

    public Triplet(Tuple t, int b, int o, int e, int m){
        this.tuple = t;
        this.blockNumber = b;
        this.tupleOffset = o;
        this.endBlock = e;
        this.memBlock = m;
    }
}
