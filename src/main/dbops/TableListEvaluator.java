package main.dbops;

import java.util.ArrayList;
import java.util.List;
import main.parsetree.TreeNode;
import storageManager.Tuple;

public class TableListEvaluator {

    public List<String> getTableNames(TreeNode tablesList){
        List<String> tableNames = new ArrayList<>();
        List<TreeNode> children = tablesList.getChildren();

        for(int i=0; i < children.size(); i+=2){
            tableNames.add(children.get(i).getChildren().get(0).getValue());
        }
        return tableNames;
    }

}
