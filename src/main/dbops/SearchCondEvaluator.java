package main.dbops;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import main.parsetree.TreeNode;
import main.utils.TreeUtils;
import storageManager.Tuple;

// ***I MAY BE TOTALLY WRONG. This is just my initial thinking.**

// Sandeep: evaluate function below is a placeholder for what I think should be
// done for week 2. Feel free to change the function signature as you see fit.

// This is what I understand: if you see the "Week 3" comments in the problem sheet,
// it says, "You will need to modify the whole project"
// In the evaluate function, we will be performing the WHERE clause check on the
// parse tree. Whereas, it should be done on the logical tree later.
// So, I guess we will be using snippets of code from here when we actually implement
// the logical tree stuff.

public class SearchCondEvaluator {


    // Need to test this.
    public List<Tuple> evaluate(List<Tuple> table, TreeNode node){
        List<Tuple> tuples = new ArrayList<>();
        if(node == null){
            return table;
        }

        if(!node.getValue().equals("search-condition")){
            print("WHERE clause evaluation can be evaluated only on a search condition node.");
            return null;
        }
        List<TreeNode> booleanTerms = node.getChildren();
        for(int k=0; k < booleanTerms.size(); k +=2){
            TreeNode boolTerm = booleanTerms.get(k);
            if(!boolTerm.getValue().equals("boolean-term")){
                print("Expected boolean-term. Found: " + boolTerm.getValue());
                return null;
            }
            if(!evaluateBooleanTerm(table, boolTerm, tuples)){
                print("Error evaluating boolean term");
                return null;
            }
        }
        return tuples;
    }

    public boolean evaluateBooleanTerm(List<Tuple> table, TreeNode node, List<Tuple> tuples){
        List<TreeNode> booleanFactors = node.getChildren();
        HashSet<Tuple> result = new HashSet<>();

        if(!evaluateBooleanFactor(table, booleanFactors.get(0), result)){
            print("Error evaluating first boolean factor.");
            return false;
        }

        for(int i=2; i < booleanFactors.size(); i+=2){
            HashSet<Tuple> set = new HashSet<>();
            if(!evaluateBooleanFactor(table, booleanFactors.get(i), set)){
                print("Error evaluating boolean factor.");
                return false;
            }
            intersect(result, set);
        }

        for(Tuple res: result){
            tuples.add(res);
        }
        return true;
    }

    public boolean evaluateBooleanFactor(List<Tuple> table, TreeNode node, HashSet<Tuple> set){
        List<TreeNode> children = node.getChildren();
        TreeNode expr1 = children.get(0);
        TreeNode expr2 = children.get(2);

        String solveExpr1 = solveType(expr1);
        String solveExpr2 = solveType(expr2);
        String op = children.get(1).getChildren().get(0).getValue();

        if(solveExpr1.equals("column-name")){
            String colName1 = getColName(expr1);
            if(solveExpr2.equals("column-name")){
                String colName2 = getColName(expr2);
                if(!columnMatchTuples(table, colName1, colName2, set, op)){
                    return false;
                }
            }else if(solveExpr2.equals("literal")){
                String literal2 = getLiteralVal(expr2);
                if(!columnMatchLiterals(table, colName1, literal2, set, op)){
                    return false;
                }
            }else if(solveExpr2.equals("integer")){
                int val2 = getIntVal(expr2);
                if(!columnMatchInt(table, colName1, val2, set, op)){
                    return false;
                }
            }else{
                print("Expression type mismatch."); return false;
            }

        }else if(solveExpr1.equals("literal")){
            String literal1 = getLiteralVal(expr1);

            if(solveExpr2.equals("column-name")){
                String colName2 = getColName(expr2);
                if(!columnMatchLiterals(table, colName2, literal1, set, op)){
                    return false;
                }
            }else if(solveExpr2.equals("literal")){
                String literal2 = getLiteralVal(expr2);
                print("Trying to match two literals."); return false;
            }else if(solveExpr2.equals("integer")){
                int val2 = getIntVal(expr2);
                print("Trying to match literal with int."); return false;
            }else{
                print("Expression type mismatch."); return false;
            }

        }else if(solveExpr1.equals("integer")){
            int val1 = getIntVal(expr2);

            if(solveExpr2.equals("column-name")){
                String colName2 = getColName(expr2);
                if(!columnMatchInt(table, colName2, val1, set, op)){
                    return false;
                }
            }else if(solveExpr2.equals("literal")){
                String literal2 = getLiteralVal(expr2);
                print("Trying to match literal with int."); return false;
            }else if(solveExpr2.equals("integer")){
                int val2 = getIntVal(expr2);
                print("Trying to match two integers."); return false;
            }else{
                print("Expression type mismatch."); return false;
            }

        }else if(solveExpr1.equals("colplus")){
            String colName1 = getName(expr1).split(";")[0];
            String colName2 = getName(expr1).split(";")[1];
            int val2 = getIntVal(expr2);
            if(!columnMatchValuesPlus(table, colName1, colName2, val2, set, op)){
                return false;
            }
        }else if(solveExpr1.equals("colminus")) {
            String colName1 = getName(expr1).split(";")[0];
            String colName2 = getName(expr1).split(";")[1];
            int val2 = getIntVal(expr2);
            if(!columnMatchValuesMinus(table, colName1, colName2, val2, set, op)){
                return false;
            }
        }else{
            print("Expression type mismatch."); return false;
        }

        return true;
    }

    public void intersect(HashSet<Tuple> result, HashSet<Tuple> set){
        HashSet<Tuple> res = new HashSet<>();
        for(Tuple tuple: set){
            if(result.contains(tuple)){
                res.add(tuple);
            }
        }
        result.clear();
        for(Tuple tuple: res){
            result.add(tuple);
        }
    }

    public String solveType(TreeNode expr){
        List<TreeNode> children = expr.getChildren();
        if(children.size() == 1){
            if(children.get(0).getChildren().get(0).getValue().equals("column-name")){
                return "column-name";
            }else if(children.get(0).getChildren().get(0).getValue().equals("literal")){
                return "literal";
            }else if(children.get(0).getChildren().get(0).getValue().equals("integer")){
                return "integer";
            }else{
                return "";
            }
        }else if(children.size() == 5){
            if(children.get(1).getChildren().get(0).getValue().equals("integer") &&
                    children.get(3).getChildren().get(0).getValue().equals("integer")){
                if(children.get(2).getValue().equals("+")){
                    return "integer";
                }else if(children.get(2).getValue().equals("-")){
                    return "integer";
                }else{
                    print("Wrong terms."); return "";
                }
            }else if (children.get(1).getChildren().get(0).getValue().equals("column-name") &&
                    children.get(3).getChildren().get(0).getValue().equals("column-name")) {
                if(children.get(2).getValue().equals("+")){
                    return "colplus";
                }else if(children.get(2).getValue().equals("-")){
                    return "colminus";
                }else{
                    print("Wrong terms."); return "";
                }
            }else{
                return "";
            }
        }else{
            return "";
        }
    }

    public int getIntVal(TreeNode expr){
        List<TreeNode> children = expr.getChildren();
        if(children.size() == 1){
            return Integer.parseInt(expr.getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue());
        }else{
            // it will be 5
            int val1 = Integer.parseInt(children.get(1).getChildren().get(0).getChildren().get(0).getValue());
            int val2 = Integer.parseInt(children.get(3).getChildren().get(0).getChildren().get(0).getValue());
            if(children.get(2).getChildren().get(0).getValue().equals("+")){
                return val1 + val2;
            }else {
                return val1 - val2;
            }
        }
    }

    public String getColName(TreeNode expr){
        try{
            String ret = "";
            if(expr.getChildren().get(0).getChildren().get(0).getChildren().size() == 1){
                ret = expr.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue();
            }else{
                ret = expr.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue() +
                        "." +
                        expr.getChildren().get(0).getChildren().get(0).getChildren().get(1).getChildren().get(0).getValue();
            }
            return ret;
        }catch (NullPointerException e){
            print("Couldn't get col name."); return "";
        }
    }

    private String getName(TreeNode expr){
        try{
            String ret = "";
            if(expr.getChildren().get(1).getChildren().get(0).getChildren().size() == 1){
                ret = expr.getChildren().get(1).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue();
            }else{
                ret = expr.getChildren().get(1).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue() +
                        "." +
                        expr.getChildren().get(1).getChildren().get(0).getChildren().get(1).getChildren().get(0).getValue();
            }
            ret += ";";
            if(expr.getChildren().get(3).getChildren().get(0).getChildren().size() == 1){
                ret += expr.getChildren().get(3).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue();
            }else{
                ret += expr.getChildren().get(3).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue() +
                        "." +
                        expr.getChildren().get(3).getChildren().get(0).getChildren().get(1).getChildren().get(0).getValue();
            }
            return ret;
        }catch (NullPointerException e){
            print("Couldn't get col name."); return "";
        }
    }

    public String getLiteralVal(TreeNode expr){
        String ret = expr.getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue();
        return ret.substring(1, ret.length()-1);
    }

    public boolean columnMatchTuples(List<Tuple> table, String col1, String col2, HashSet<Tuple> set, String op){
        for(Tuple tuple: table){
            if(op.equals("=")){
                if(tuple.getField(col1).toString().equals(tuple.getField(col2).toString())){
                    set.add(tuple);
                }
            }else if(op.equals("<")){
                if(tuple.getField(col1).toString().compareTo(tuple.getField(col2).toString()) < 0){
                    set.add(tuple);
                }
            }else if(op.equals(">")){
                if(tuple.getField(col1).toString().compareTo(tuple.getField(col2).toString()) > 0){
                    set.add(tuple);
                }
            }else{
                print("Op not supported"); return false;
            }
        }
        return true;
    }

    public boolean columnMatchLiterals(List<Tuple> table, String col, String lit, HashSet<Tuple> set, String op){
        for(Tuple tuple: table){
            if(op.equals("=")){
                if(tuple.getField(col).toString().equals(lit)){
                    set.add(tuple);
                }
            }else if(op.equals("<")){
                if(tuple.getField(col).toString().compareTo(lit) < 0){
                    set.add(tuple);
                }
            }else if(op.equals(">")){
                if(tuple.getField(col).toString().compareTo(lit) > 0){
                    set.add(tuple);
                }
            }else{
                print("Op not supported"); return false;
            }
        }
        return true;
    }

    public boolean columnMatchInt(List<Tuple> table, String col, int val, HashSet<Tuple> set, String op){
        for(Tuple tuple: table){
            if(op.equals("=")){
                if(Integer.parseInt(tuple.getField(col).toString()) == val){
                    set.add(tuple);
                }
            }else if(op.equals("<")){
                if(Integer.parseInt(tuple.getField(col).toString()) < val){
                    set.add(tuple);
                }
            }else if(op.equals(">")){
                if(Integer.parseInt(tuple.getField(col).toString()) > val){
                    set.add(tuple);
                }
            }else{
                print("Op not supported"); return false;
            }

        }
        return true;
    }

    public boolean columnMatchValuesPlus(List<Tuple> table, String col1, String col2, int val, HashSet<Tuple> set, String op){
        for(Tuple tuple: table){
            if(op.equals("=")){
                if(Integer.parseInt(tuple.getField(col1).toString()) + Integer.parseInt(tuple.getField(col2).toString()) == val){
                    set.add(tuple);
                }
            }else if(op.equals("<")){
                if(Integer.parseInt(tuple.getField(col1).toString()) + Integer.parseInt(tuple.getField(col2).toString()) < val){
                    set.add(tuple);
                }
            }else if(op.equals(">")){
                if(Integer.parseInt(tuple.getField(col1).toString()) + Integer.parseInt(tuple.getField(col2).toString()) > val){
                    set.add(tuple);
                }
            }else{
                print("Op not supported"); return false;
            }

        }
        return true;
    }

    public boolean columnMatchValuesMinus(List<Tuple> table, String col1, String col2, int val, HashSet<Tuple> set, String op){
        for(Tuple tuple: table){
            if(op.equals("=")){
                if(Integer.parseInt(tuple.getField(col1).toString()) - Integer.parseInt(tuple.getField(col2).toString()) == val){
                    set.add(tuple);
                }
            }else if(op.equals("<")){
                if(Integer.parseInt(tuple.getField(col1).toString()) - Integer.parseInt(tuple.getField(col2).toString()) < val){
                    set.add(tuple);
                }
            }else if(op.equals(">")){
                if(Integer.parseInt(tuple.getField(col1).toString()) - Integer.parseInt(tuple.getField(col2).toString()) > val){
                    set.add(tuple);
                }
            }else{
                print("Op not supported"); return false;
            }

        }
        return true;
    }

    public void print(String s){
        System.out.println(s);
    }
}
