package main.dbops;

import java.util.ArrayList;
import java.util.List;
import main.parsetree.TreeNode;
import storageManager.FieldType;
import storageManager.Schema;

public class AttrListSolver {

    public Schema generateSchemaforAttributeTypeList(TreeNode attrList){
        if(attrList == null || !attrList.getValue().equals("attribute-type-list")){
            return null;
        }

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<FieldType> fieldValues = new ArrayList<>();
        List<TreeNode> children = attrList.getChildren();
        for(int i=0; i < children.size(); i+=3){
            String fieldName = children.get(i).getChildren().get(0).getValue();
            String fieldValue = children.get(i+1).getChildren().get(0).getValue();

            fieldNames.add(fieldName);
            if(fieldValue.equals("INT")){
                fieldValues.add(FieldType.INT);
            }else if(fieldValue.equals("STR20")){
                fieldValues.add(FieldType.STR20);
            }else{
                System.out.println("Error. Incorrect field value.");
                return null;
            }
        }
        Schema schema = new Schema(fieldNames, fieldValues);
        return schema;
    }

    public List<String> generateSchemaForAttrList(TreeNode attributeList){
        if(attributeList == null || !attributeList.getValue().equals("attribute-list")){
            return null;
        }
        ArrayList<String> fieldNames = new ArrayList<>();
        List<TreeNode> children = attributeList.getChildren();
        for(int i=0; i < children.size(); i+=2){
            String fieldName = children.get(i).getChildren().get(0).getValue();
            fieldNames.add(fieldName);
        }
        return fieldNames;
    }
}
