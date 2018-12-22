package main;

import main.dbops.*;
import main.parser.LQPGenerator;
import main.parser.QueryParser;
import main.parsetree.*;
import storageManager.*;

import java.util.ArrayList;
import java.util.List;

public class TinySQL {

    private List<Tuple> output;
    private DbProcedures impl;

    public TinySQL(){
        output = new ArrayList<>();

        MainMemory mem = new MainMemory();
        Disk disk = new Disk();
        SchemaManager manager = new SchemaManager(mem, disk);

        impl = new DbProcedures(mem, disk, manager);
    }

    public String preProcess(String sql){
        if(sql.charAt(sql.length()-1) != ';'){
            return sql + ';';
        }
        return sql;
    }

    public void evaluate(String sql, boolean isBasicSelect){
        impl.reset();
        sql = preProcess(sql);
        QueryParser parser = new QueryParser();
        TreeNode root = parser.parse(sql);

        if(root == null){
            System.out.println("Unable to parse SQL statement.");
            return;
        }

        if(root.getValue().equals("create-table-statement")){

            try {
                TreeNode attrList = root.getChildren().get(4);

                AttrListSolver attrListSolver = new AttrListSolver();
                String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
                Schema schema = attrListSolver.generateSchemaforAttributeTypeList(attrList);

                if(impl.createTable(relationName, schema)){
                    print("Table created.");
                }else{
                    print("Error creating table.");
                }
            } catch (Exception e){
                print("Exception caught when creating table. Please check.");
                e.printStackTrace();
                return;
            }

        }else if(root.getValue().equals("drop-table-statement")){

            try {
                if (impl.dropTable(root.getChildren().get(2).getChildren().get(0).getValue())) {
                    print("Expected: Table dropped.");
                } else {
                    print("Error dropping table.");
                }
            }catch (Exception e){
                print("Exception caught when dropping table. Please check.");
                e.printStackTrace();
                return;
            }

        }else if(root.getValue().equals("select-statement")){
            if(isBasicSelect){
                try {
                    TableListEvaluator tableListEvaluator = new TableListEvaluator();
                    List<String> tables = tableListEvaluator.getTableNames(root.getChildren().get(3));

                    List<Tuple> bigTable = impl.tablesCrossProduct(tables);
                    SearchCondEvaluator searchCondEvaluator = new SearchCondEvaluator();

                    List<Tuple> result;
                    if(root.getChildren().size() != 4){
                        result = searchCondEvaluator.evaluate(bigTable, root.getChildren().get(5));
                    }else{
                        result = new ArrayList<>(bigTable);
                    }

                    print("----------RESULT FOR SELECT QUERY (BEGIN)---------");
                    print(sql);
                    System.out.println("Result size: " + result.size());
                    ArrayList<String> colNames = new ArrayList<>();
                    impl.getFNames(colNames);

                    for(String str: colNames){
                        System.out.print(str + "  ");
                    }
                    System.out.println();
                    for(Tuple t: result){
                        output.add(t);
                        for(String col: colNames){
                            if(t.getField(col).toString().equals(Integer.toString(Integer.MIN_VALUE))){
                                System.out.print("null  ");
                            }else {
                                System.out.print(t.getField(col).toString() + "  ");
                            }
                        }
                        System.out.println();
                        // print(t.toString());
                    }
                    print("----------RESULT FOR SELECT QUERY (END)---------");
                }catch (Exception e){
                    print("Exception caught when executing query.");
                    e.printStackTrace();
                    return;
                }
            } else {
                LQPGenerator generator = new LQPGenerator(root);
                generator.generateLqp();
                LQPNode node = generator.getLqpTree();

                List<Tuple> result = new ArrayList<>();
                if(node instanceof ProjectionNode){
                    ((ProjectionNode) node).execute(true, impl, result);
                }else if(node instanceof DuplicateEliminationNode){
                    ((DuplicateEliminationNode) node).execute(true, impl, result);
                }else if(node instanceof SortNode){
                    ((SortNode) node).execute(true, impl, result);
                }

                print("----------RESULT FOR SELECT QUERY (BEGIN)---------");
                print(sql);
                System.out.println("Result size: " + result.size());
                ArrayList<String> colNames = new ArrayList<>();
                impl.getFNames(colNames);

                for(String str: colNames){
                    System.out.print(str + "  ");
                }
                System.out.println();
                for(Tuple t: result){
                    output.add(t);
                    for(String col: colNames){
                        if(t.getField(col).toString().equals(Integer.toString(Integer.MIN_VALUE))){
                            System.out.print("null  ");
                        }else {
                            System.out.print(t.getField(col).toString() + "  ");
                        }
                    }
                    System.out.println();
                    // print(t.toString());
                }
                print("----------RESULT FOR SELECT QUERY (END)---------");
                print("Number of DiskIOs: " + impl.getDiskIO());
            }

        }else if(root.getValue().equals("insert-table-statement")){
            try{
                String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
                AttrListSolver attrListSolver = new AttrListSolver();
                List<String> columns = attrListSolver.generateSchemaForAttrList(root.getChildren().get(4));

                InsertTuplesSolver insertTuplesSolver = new InsertTuplesSolver();
                List<Object> fieldValues = insertTuplesSolver.generateFieldValues(root.getChildren().get(6));


                if(fieldValues.size() == 0){
                    // the insert-tuples is a select statement
                    if(isBasicSelect){
                        TreeNode node = root.getChildren().get(6).getChildren().get(0);
                        TableListEvaluator tableListEvaluator = new TableListEvaluator();
                        List<String> tables = tableListEvaluator.getTableNames(node.getChildren().get(3));

                        List<Tuple> bigTable = impl.tablesCrossProduct(tables);
                        SearchCondEvaluator searchCondEvaluator = new SearchCondEvaluator();

                        List<Tuple> result;
                        if(node.getChildren().size() != 4){
                            result = searchCondEvaluator.evaluate(bigTable, node.getChildren().get(5));
                        }else{
                            result = new ArrayList<>(bigTable);
                        }

                        for(Tuple t: result){
                            List<Object> fv = new ArrayList<>();
                            for(String col: columns){
                                if(t.getField(col).type == FieldType.INT){
                                    fv.add(Integer.parseInt(t.getField(col).toString()));
                                }else if(t.getField(col).type == FieldType.STR20){
                                    fv.add(t.getField(col).toString());
                                }else{
                                    fv.add(null);
                                }
                            }

                            if(impl.insertTuple(relationName, columns, fv)){
                                print("Tuple inserted.");
                            }else{
                                print("Error encountered while inserting.");
                                return;
                            }
                        }
                    } else {
                        TreeNode node = root.getChildren().get(6).getChildren().get(0);
                        LQPGenerator generator = new LQPGenerator(node);
                        generator.generateLqp();
                        LQPNode lqpNode = generator.getLqpTree();

                        String tempRelName = "";
                        if(lqpNode instanceof ProjectionNode){
                            tempRelName = ((ProjectionNode) lqpNode).execute(false, impl, null);
                        }
                        if(impl.insertAllFromAToB(tempRelName, relationName)){
                            print("All tuples inserted");
                        }else{
                            print("Insert failed");
                        }
                    }
                }else{
                    if(impl.insertTuple(relationName, columns, fieldValues)){
                        print("Tuple inserted.");
                    }else{
                        print("Error encountered.");
                        return;
                    }
                }
            }catch (Exception e){
                print("Exception while performing insert statement.");
                e.printStackTrace();
                return;
            }
        }else if(root.getValue().equals("delete-table-statement")){
            try {
                String relationName = root.getChildren().get(2).getChildren().get(0).getValue();
                TreeNode searchCondition = null;
                if(root.getChildren().size() > 4){
                    searchCondition = root.getChildren().get(4);
                }

                if(!impl.deleteFromTable(relationName, searchCondition)){
                    print("Unable to delete from table.");
                    return;
                }else{
                    print("Delete successful.");
                }
            }catch (Exception e){
                print("Exception caught while deleting.");
                e.printStackTrace();
            }
        }else{
            System.out.println("Unable to parse SQL statement. Root node incorrect.");
            return;
        }
    }

    private void print(String s){
        System.out.println(s);
    }
}
