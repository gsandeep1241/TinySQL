package main.parsetree;

import main.dbops.DbProcedures;

import java.util.*;

/**
 * LQP Node class for join operation
 * SELECT FROM table1, table2; syntax ONLY does cross-joins
 */
public class JoinNode extends LQPNode {
    private TreeNode condition;

    public JoinNode() {
        // this.condition will be set by optimizer only.
        // No need to set it from sqlTree like other nodes,
        // because TinySQL doesn't support theta-joins out of the box.
    }

    @Override
    public String toString() {
        return String.format("❌(%s)", "");
    }

    /**
     * Cross Joins tables returned by this.getChildren().
     */
    public String execute(boolean getOutput, DbProcedures impl, List<String> res) {
        List<LQPNode> children = this.getChildren();
        HashSet<Dual> relationNames = new HashSet<>();

        for(int i=0; i < children.size(); i++){
            if(children.get(i) instanceof ScanNode){
                String rel = ((ScanNode)children.get(i)).execute();
                int size = impl.getSize(rel);
                relationNames.add(new Dual(rel, size));
            }else if(children.get(i) instanceof SelectionNode){
                String rel = ((SelectionNode)children.get(i)).execute(impl);
                int size = impl.getSize(rel);
                relationNames.add(new Dual(rel, size));
            }else if(children.get(i) instanceof JoinNode){
                String rel = ((JoinNode)children.get(i)).execute(false, impl, null);
                int size = impl.getSize(rel);
                relationNames.add(new Dual(rel, size));
            }
        }

        String colName1 = null;
        String colName2 = null;

        while(relationNames.size() != 1){
            Dual d1 = getFirstMin(relationNames);
            Dual d2 = getSecondMin(relationNames);
            String r1 = d1.name;
            String r2 = d2.name;
            String temp = impl.twoPassJoin(r1, r2, condition, null, null);
            int size = impl.getSize(temp);
            relationNames.add(new Dual(temp, size));
            relationNames.remove(d1); relationNames.remove(d2);


        }
        ArrayList<Dual> relN = new ArrayList<>(relationNames);
        return relN.get(0).name;
    }

    private Dual getFirstMin(HashSet<Dual> relationNames){
        ArrayList<Dual> list = new ArrayList<>(relationNames);
        Collections.sort(list, new Comparator<Dual>() {
            @Override
            public int compare(Dual o1, Dual o2) {
                return o1.size-o2.size;
            }
        });
        return list.get(0);
    }

    private Dual getSecondMin(HashSet<Dual> relationNames){
        ArrayList<Dual> list = new ArrayList<>(relationNames);
        Collections.sort(list, new Comparator<Dual>() {
            @Override
            public int compare(Dual o1, Dual o2) {
                return o1.size-o2.size;
            }
        });
        return list.get(1);
    }

    public TreeNode getCondition() {
        return condition;
    }

    public void setCondition(TreeNode condition) {
        this.condition = condition;
    }
}

class Dual {
    int size;
    String name;

    public Dual(String s, int n){
        name = s;
        size = n;
    }
}
