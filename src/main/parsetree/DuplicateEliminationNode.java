package main.parsetree;

import main.dbops.DbProcedures;
import storageManager.Tuple;

import java.util.List;

/**
 * LQP Node class for delta operation - duplicate elimination
 *
 * @attr: eliminationEnabled: switch to turn the node on/off - this will come in handy in advanced use cases
 * Can be initialized with sqlTree (where it looks for DISTINCT); or simple boolean (for use in set operations)
 */
public class DuplicateEliminationNode extends LQPNode {
    private Boolean eliminationEnabled;

    public DuplicateEliminationNode(Boolean eliminationEnabled) {
        this.eliminationEnabled = eliminationEnabled;
    }

    public DuplicateEliminationNode(TreeNode sqlTree) {
        this.eliminationEnabled = sqlTree.getChildren().get(1).getValue().equals("DISTINCT");
    }

    @Override
    public String toString() {
        return String.format(
                "δ(%s) %s",
                this.getChildren().size() >= 1 ? this.getChild().toString() : "",
                this.eliminationEnabled ? "ACTIVE" : "NOT ACTIVE"
        );
    }

    public Boolean getEliminationEnabled() {
        return eliminationEnabled;
    }

    public void setEliminationEnabled(Boolean eliminationEnabled) {
        this.eliminationEnabled = eliminationEnabled;
    }

    /**
     * Depending on this.eliminationEnabled, eliminates all duplicates or does nothing.
     * Operates on table returned by this.getChild().
     */
    public String execute(boolean isFirst, DbProcedures impl, List<Tuple> output) {
        LQPNode child = this.getChild();

        String relation = "";

        if(child instanceof ProjectionNode){
            String tempRelation = ((ProjectionNode) child).execute(false, impl, null);
            relation = impl.onePassDupElimination(tempRelation, isFirst, output);
        }
        return relation;
    }
}
