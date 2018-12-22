package main.parsetree;

import main.dbops.DbProcedures;

/**
 * LQP Node class for selection operation
 * @attr: Condition: Just a TreeNode now. TODO: Parse it into executable boolean expression
 */
public class SelectionNode extends LQPNode {
    private TreeNode condition;

    public SelectionNode(TreeNode sqlTree) {
        this.condition = sqlTree.getChildren().stream()
                .filter(tn -> "search-condition".equals(tn.getValue()))
                .findAny()
                .orElse(null);
    }

    public TreeNode getCondition() {
        return condition;
    }

    @Override
    public String toString() {
        return String.format("σ(%s)", this.condition == null ? "" : this.condition.toString());
    }

    /**
     * Parses this.condition into executable boolean expression,
     * then applies the condition in each tuple in table returned in getChild()
     */
    public String execute(DbProcedures impl) {
        LQPNode child = this.getChild();

        String relation = "";
        if(child instanceof ScanNode){
            String tempRelation = ((ScanNode) child).execute();
            relation = impl.onePassSelection(tempRelation, condition);
        }else if(child instanceof JoinNode){
            String tempRelation = ((JoinNode) child).execute(false, impl, null);
            relation = impl.onePassSelection(tempRelation, condition);
        }
        return relation;
    }
}
