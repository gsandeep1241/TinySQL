package main.dbops;

import main.parsetree.TreeNode;
import main.utils.TreeUtils;

import java.util.ArrayList;
import java.util.List;

public class InsertTuplesSolver {

    public List<Object> generateFieldValues(TreeNode insertTuples){
        if(insertTuples == null || !insertTuples.getValue().equals("insert-list")){
            return null;
        }

        if(insertTuples.getChildren().size() == 1){
            // select statement
            return new ArrayList<>();
        }

        TreeNode valuesList = insertTuples.getChildren().get(2);
        List<TreeNode> values = valuesList.getChildren();

        List<Object> res = new ArrayList<>();

        for(int i=0; i < values.size(); i+=2){
            if(values.get(i).getChildren().get(0).getValue().equals("NULL")){
                res.add(null);
            }else if(values.get(i).getChildren().get(0).getValue().equals("integer")){
                res.add(Integer.parseInt(values.get(i).getChildren().get(0).getChildren().get(0).getValue()));
            }else if(values.get(i).getChildren().get(0).getValue().equals("literal")){
                String str = values.get(i).getChildren().get(0).getChildren().get(0).getValue();
                res.add(str.substring(1, str.length()-1));
            }
        }

        return res;
    }
}
