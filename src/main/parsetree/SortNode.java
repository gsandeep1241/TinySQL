package main.parsetree;

import main.dbops.DbProcedures;
import storageManager.Tuple;
import sun.reflect.generics.tree.Tree;

import java.util.List;

/**
 * LQP Node class for tau operation - sorting
 * @attr sortingEnabled: switch to turn the node on/off
 * @attr columnName: column name to sort the table on
 */
public class SortNode extends LQPNode {
    private Boolean sortingEnabled;
    private String columnName;

    public SortNode(TreeNode sqlTree) {
        TreeNode orderBy = sqlTree.getChildren().stream()
                .filter(tn -> "ORDER BY".equals(tn.getValue()))
                .findFirst()
                .orElse(null);
        if (orderBy == null) {
            this.sortingEnabled = false;
            this.columnName = null;
        } else {
            this.sortingEnabled = true;
            // last child is the columnName node
            TreeNode columnName = sqlTree.getChildren().get(sqlTree.getChildren().size() - 1);
            // column-name/attribute-name/actual-column-name-in-relation
            if(columnName.getChildren().size() == 1){
                this.columnName = columnName.getChildren().get(0).getChildren().get(0).getValue();
            }else{
                this.columnName = columnName.getChildren().get(0).getChildren().get(0).getValue() + "." +
                        columnName.getChildren().get(1).getChildren().get(0).getValue();
            }
        }
    }

    @Override
    public String toString() {
        return String.format("τ(%s:%s)", this.columnName, this.sortingEnabled ? "ACTIVE" : "NOT ACTIVE");
    }

    /**
     * Sorts the table based on this.columnName. Does nothing if this.sortingEnabled = false
     * Operates on table returned by this.getChild().
     */
    public String execute(boolean isFirst, DbProcedures impl, List<Tuple> output) {
        LQPNode child = this.getChild();

        String relation = "";

        if(child instanceof ProjectionNode){
            String tempRelation = ((ProjectionNode) child).execute(false, impl, null);
            relation = impl.onePassSorting(tempRelation, isFirst, output, columnName);
        }else if(child instanceof DuplicateEliminationNode){
            String tempRelation = ((DuplicateEliminationNode) child).execute(false, impl, null);
            relation = impl.onePassSorting(tempRelation, isFirst, output, columnName);
        }
        return relation;
    }

    public String getColumnName() {
        return columnName;
    }

    public Boolean getSortingEnabled() {
        return sortingEnabled;
    }
}
