package main;

import java.util.ArrayList;
import java.util.List;

import main.dbops.AttrListSolver;
import main.dbops.DbProcedures;
import main.dbops.SearchCondEvaluator;
import main.dbops.TableListEvaluator;
import main.parser.QueryParser;
import main.parsetree.TreeNode;
import storageManager.*;

public class TopDownMain {

    public static void main(String[] args) {
        print("Welcome to TinySQL!");

        // testCase1();
        // testCase2();
        // whereClauseTest();
        //onePassDupElimTest();
        // onePassSortTest();
    }

    public static void testCase1() {
        MainMemory mem = new MainMemory();
        Disk disk = new Disk();
        SchemaManager manager = new SchemaManager(mem, disk);
        DbProcedures impl = new DbProcedures(mem, disk, manager);

        print("Executing test case 1..");
        QueryParser parser = new QueryParser();
        AttrListSolver attrListSolver = new AttrListSolver();

        String sql = "CREATE TABLE teams(id INT, name STR20);";
        TreeNode root = parser.parse(sql);

        TreeNode attrList = root.getChildren().get(4);
        String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
        Schema schema = attrListSolver.generateSchemaforAttributeTypeList(attrList);

        if (impl.createTable(relationName, schema)) {
            print("Expected: Table created.");
        } else {
            print("Error creating table.");
        }

        ArrayList<String> fieldNames = schema.getFieldNames();
        ArrayList<Object> fieldValues = new ArrayList<>();
        fieldValues.add(1);
        fieldValues.add("teamA");

        if (impl.insertTuple("teams", fieldNames, fieldValues)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        String sqlDrop = "DROP TABLE teams;";
        root = parser.parse(sqlDrop);


        if (impl.dropTable(root.getChildren().get(2).getChildren().get(0).getValue())) {
            print("Expected: Table dropped.");
        } else {
            print("Error dropping table.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues)) {
            print("Tuple inserted.");
        } else {
            print("Expected: Error inserting tuple.");
        }
    }

    public static void testCase2() {
        print("Executing test case 2..");
        MainMemory mem = new MainMemory();
        Disk disk = new Disk();
        SchemaManager manager = new SchemaManager(mem, disk);
        DbProcedures impl = new DbProcedures(mem, disk, manager);

        QueryParser parser = new QueryParser();
        AttrListSolver attrListSolver = new AttrListSolver();
        String sql = "CREATE TABLE teams(id INT, name STR20);";
        TreeNode root = parser.parse(sql);

        TreeNode attrList = root.getChildren().get(4);
        String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
        Schema schema = attrListSolver.generateSchemaforAttributeTypeList(attrList);

        if (impl.createTable(relationName, schema)) {
            print("Expected: Table created.");
        } else {
            print("Error creating table.");
        }

        ArrayList<String> fieldNames = schema.getFieldNames();

        ArrayList<Object> fieldValues1 = new ArrayList<>();
        fieldValues1.add(1);
        fieldValues1.add("teamA");

        ArrayList<Object> fieldValues2 = new ArrayList<>();
        fieldValues2.add(2);
        fieldValues2.add("teamB");

        ArrayList<Object> fieldValues3 = new ArrayList<>();
        fieldValues3.add(3);
        fieldValues3.add("teamC");

        if (impl.insertTuple("teams", fieldNames, fieldValues1)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues2)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues3)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        List<Tuple> tuples = impl.selectAllFromTable("teams");
        print("List of tuples..");
        for (Tuple tuple : tuples) {
            print(tuple.toString());
        }
        String sqlDrop = "DROP TABLE teams;";
        root = parser.parse(sqlDrop);


        if (impl.dropTable(root.getChildren().get(2).getChildren().get(0).getValue())) {
            print("Expected: Table dropped.");
        } else {
            print("Error dropping table.");
        }
    }

    public static void whereClauseTest(){
        print("Executing test case 3..");
        MainMemory mem = new MainMemory();
        Disk disk = new Disk();
        SchemaManager manager = new SchemaManager(mem, disk);
        DbProcedures impl = new DbProcedures(mem, disk, manager);

        QueryParser parser = new QueryParser();
        AttrListSolver attrListSolver = new AttrListSolver();
        String sql1 = "CREATE TABLE teams1(id1 INT, name1 STR20);";
        TreeNode root = parser.parse(sql1);

        TreeNode attrList = root.getChildren().get(4);
        String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
        Schema schema = attrListSolver.generateSchemaforAttributeTypeList(attrList);

        if(impl.createTable(relationName, schema)){
            print("Expected: Table created.");
        }else{
            print("Error creating table.");
        }

        ArrayList<String> fieldNames = schema.getFieldNames();

        ArrayList<Object> fieldValues1 = new ArrayList<>();
        fieldValues1.add(1);
        fieldValues1.add("teamA");

        ArrayList<Object> fieldValues2 = new ArrayList<>();
        fieldValues2.add(2);
        fieldValues2.add("teamB");

        ArrayList<Object> fieldValues3 = new ArrayList<>();
        fieldValues3.add(3);
        fieldValues3.add("teamC");

        if(impl.insertTuple("teams1", fieldNames, fieldValues1)){
            print("Expected: Tuple inserted.");
        }else{
            print("Error inserting tuple.");
        }

        if(impl.insertTuple("teams1", fieldNames, fieldValues2)){
            print("Expected: Tuple inserted.");
        }else{
            print("Error inserting tuple.");
        }

        if(impl.insertTuple("teams1", fieldNames, fieldValues3)){
            print("Expected: Tuple inserted.");
        }else {
            print("Error inserting tuple.");
        }

        String sql2 = "CREATE TABLE teams2(id2 INT, name2 STR20);";
        root = parser.parse(sql2);

        attrList = root.getChildren().get(4);
        relationName = root.getChildren().get(2).getChildren().get(0).getValue();
        schema = attrListSolver.generateSchemaforAttributeTypeList(attrList);

        if(impl.createTable(relationName, schema)){
            print("Expected: Table created.");
        }else{
            print("Error creating table.");
        }

        ArrayList<String> fieldNames2 = schema.getFieldNames();

        ArrayList<Object> fieldValues21 = new ArrayList<>();
        fieldValues21.add(4);
        fieldValues21.add("teamD");

        ArrayList<Object> fieldValues22 = new ArrayList<>();
        fieldValues22.add(5);
        fieldValues22.add("teamE");

        ArrayList<Object> fieldValues23 = new ArrayList<>();
        fieldValues23.add(6);
        fieldValues23.add("teamF");

        if(impl.insertTuple("teams2", fieldNames2, fieldValues21)){
            print("Expected: Tuple inserted.");
        }else{
            print("Error inserting tuple.");
        }

        if(impl.insertTuple("teams2", fieldNames2, fieldValues22)){
            print("Expected: Tuple inserted.");
        }else{
            print("Error inserting tuple.");
        }

        if(impl.insertTuple("teams2", fieldNames2, fieldValues23)){
            print("Expected: Tuple inserted.");
        }else {
            print("Error inserting tuple.");
        }


        String sql3 = "SELECT * FROM teams1, teams2 WHERE teams2.name2 = 'teamF' AND teams1.id1 = 2;";
        TreeNode node = parser.parse(sql3);

        TableListEvaluator tableListEvaluator = new TableListEvaluator();
        List<String> tables = tableListEvaluator.getTableNames(node.getChildren().get(3));

        System.out.print("Tables involved: ");
        for(String table: tables){
            System.out.print(table + "; ");
        }
        System.out.println();
        List<Tuple> bigTable = impl.tablesCrossProduct(tables);
        SearchCondEvaluator evaluator = new SearchCondEvaluator();

        // for ordered printing
        try {
            Thread.sleep(1000);
        }catch (Exception e){
            System.out.println("Couldn't sleep.");
        }

        List<Tuple> result = new ArrayList<>();
        if(node.getChildren().size() != 4){
            result = evaluator.evaluate(bigTable, node.getChildren().get(5));
        }else{
            result = new ArrayList<>(bigTable);
        }

        print("----------RESULT-----------");
        print(sql3);
        for(Tuple tuple: result){
            print(tuple.toString());
        }
    }

    public static void onePassDupElimTest(){
        print("Executing one pass duplicate elimination test..");
        MainMemory mem = new MainMemory();
        Disk disk = new Disk();
        SchemaManager manager = new SchemaManager(mem, disk);
        DbProcedures impl = new DbProcedures(mem, disk, manager);

        QueryParser parser = new QueryParser();
        AttrListSolver attrListSolver = new AttrListSolver();
        String sql = "CREATE TABLE teams(id INT, name STR20);";
        TreeNode root = parser.parse(sql);

        TreeNode attrList = root.getChildren().get(4);
        String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
        Schema schema = attrListSolver.generateSchemaforAttributeTypeList(attrList);

        if (impl.createTable(relationName, schema)) {
            print("Expected: Table created.");
        } else {
            print("Error creating table.");
        }

        ArrayList<String> fieldNames = schema.getFieldNames();

        ArrayList<Object> fieldValues1 = new ArrayList<>();
        fieldValues1.add(1);
        fieldValues1.add("teamA");

        ArrayList<Object> fieldValues2 = new ArrayList<>();
        fieldValues2.add(2);
        fieldValues2.add("teamB");

        ArrayList<Object> fieldValues3 = new ArrayList<>();
        fieldValues3.add(3);
        fieldValues3.add("teamC");

        ArrayList<Object> fieldValues4 = new ArrayList<>();
        fieldValues4.add(3);
        fieldValues4.add("teamC");

        ArrayList<Object> fieldValues5 = new ArrayList<>();
        fieldValues5.add(2);
        fieldValues5.add("teamB");

        if (impl.insertTuple("teams", fieldNames, fieldValues1)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues2)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues3)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues4)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues5)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        List<Tuple> tuples = new ArrayList<>();
        impl.onePassDupElimination("teams", true, tuples);
        if(tuples ==null){
            print("Error: tuples returned null.");
        }

        for(Tuple tuple: tuples){
            print(tuple.toString());
        }

        String sqlDrop = "DROP TABLE teams;";
        root = parser.parse(sqlDrop);


        if (impl.dropTable(root.getChildren().get(2).getChildren().get(0).getValue())) {
            print("Expected: Table dropped.");
        } else {
            print("Error dropping table.");
        }
    }

    public static void onePassSortTest(){
        print("Executing test case 2..");
        MainMemory mem = new MainMemory();
        Disk disk = new Disk();
        SchemaManager manager = new SchemaManager(mem, disk);
        DbProcedures impl = new DbProcedures(mem, disk, manager);

        QueryParser parser = new QueryParser();
        AttrListSolver attrListSolver = new AttrListSolver();
        String sql = "CREATE TABLE teams(id INT, name STR20);";
        TreeNode root = parser.parse(sql);

        TreeNode attrList = root.getChildren().get(4);
        String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
        Schema schema = attrListSolver.generateSchemaforAttributeTypeList(attrList);

        if (impl.createTable(relationName, schema)) {
            print("Expected: Table created.");
        } else {
            print("Error creating table.");
        }

        ArrayList<String> fieldNames = schema.getFieldNames();

        ArrayList<Object> fieldValues1 = new ArrayList<>();
        fieldValues1.add(6);
        fieldValues1.add("teamF");

        ArrayList<Object> fieldValues2 = new ArrayList<>();
        fieldValues2.add(2);
        fieldValues2.add("teamB");

        ArrayList<Object> fieldValues3 = new ArrayList<>();
        fieldValues3.add(7);
        fieldValues3.add("teamG");

        ArrayList<Object> fieldValues4 = new ArrayList<>();
        fieldValues4.add(4);
        fieldValues4.add("teamD");

        ArrayList<Object> fieldValues5 = new ArrayList<>();
        fieldValues5.add(2);
        fieldValues5.add("teamB");

        if (impl.insertTuple("teams", fieldNames, fieldValues1)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues2)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues3)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues4)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        if (impl.insertTuple("teams", fieldNames, fieldValues5)) {
            print("Expected: Tuple inserted.");
        } else {
            print("Error inserting tuple.");
        }

        List<Tuple> tuples = new ArrayList<>();
        impl.onePassSorting("teams", true, tuples, "name");
        if(tuples ==null){
            print("Error: tuples returned null.");
        }

        for(Tuple tuple: tuples){
            print(tuple.toString());
        }

        String sqlDrop = "DROP TABLE teams;";
        root = parser.parse(sqlDrop);


        if (impl.dropTable(root.getChildren().get(2).getChildren().get(0).getValue())) {
            print("Expected: Table dropped.");
        } else {
            print("Error dropping table.");
        }
    }

    public static void print(String s) {
        System.out.println(s);
    }
}
