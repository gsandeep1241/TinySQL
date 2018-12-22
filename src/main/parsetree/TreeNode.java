package main.parsetree;

import java.util.ArrayList;
import java.util.List;

public class TreeNode {

    private String value;
    private boolean isLeaf;
    private List<TreeNode> children;

    public TreeNode(String value, boolean isLeaf){
        this.value = value;
        this.isLeaf = isLeaf;
        children = new ArrayList<>();
    }

    public void addChild(TreeNode node){
        children.add(node);
    }

    public void removeChildren(){
        children = new ArrayList<>();
    }

    public void removeLastChild() {children.remove(children.size()-1);};

    public String getValue(){
        return this.value;
    }

    public List<TreeNode> getChildren(){
        return this.children;
    }

    @Override
    public String toString() {
        return String.format("NODE: %s (%d children)", this.value, this.children.size());
    }
}
