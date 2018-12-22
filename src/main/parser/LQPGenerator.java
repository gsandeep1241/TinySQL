package main.parser;

import main.parsetree.*;
import main.utils.TreeUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static main.utils.TreeUtils.convertTableListToJoinsAndScans;

public class LQPGenerator {
    private TreeNode sqlTree;   // parsed SQL tree
    private LQPNode lqpTree;    // LQP to be generated

    /**
     * @param tree: Tree for parsed SQL statement
     */
    public LQPGenerator(TreeNode tree) {
        this.sqlTree = tree;
    }

    /**
     * Converts this.sqlTree into this.lqpTree
     */
    public void generateLqp() {
        switch (this.sqlTree.getValue()) {
            case "select-statement":
                this.generateLqpForSelectStatement();
                break;
            // TODO: Add more
        }
    }

    /**
     * generateLqp for SELECT statements
     * SELECT [DISTINCT] select-list FROM table-list [WHERE search-condition] [ORDER BY column-name]
     */
    private LQPNode generateLqpForSelectStatement() {
        // tree starts at projection
        ProjectionNode projection = new ProjectionNode(this.sqlTree);
        this.lqpTree = projection;

        // followed by a selection
        SelectionNode selection = new SelectionNode(this.sqlTree);
        projection.setChild(selection);

        // then joins and scans
        TreeNode tableList = this.sqlTree.getChildren().stream()
                .filter(tn -> "table-list".equals(tn.getValue()))
                .findFirst()
                .orElse(null);

        selection.setChild(convertTableListToJoinsAndScans(tableList));


        // if there's DISTINCT, tree starts at duplicate-elimination-node
        DuplicateEliminationNode duplicateEliminationNode = new DuplicateEliminationNode(this.sqlTree);
        if (duplicateEliminationNode.getEliminationEnabled()) {
            duplicateEliminationNode.setChild(this.lqpTree);
            this.lqpTree = duplicateEliminationNode;
        }

        // if there's ORDER BY, tree starts at sort-node
        SortNode sortNode = new SortNode(this.sqlTree);
        if (sortNode.getSortingEnabled()) {
            sortNode.setChild(this.lqpTree);
            this.lqpTree = sortNode;
        }

        if(selection.getChild() instanceof ScanNode){
            projection.update();
        }

        return this.optimizeLqpForSelectStatement(this.lqpTree);
    }

    /**
     * @param lqpTree: Tree to optimize
     * @return optimized tree
     * Optimizations Performed -
     *      1. Push selections down
     *      2. Combine selections and joins
     */
    private LQPNode optimizeLqpForSelectStatement(LQPNode lqpTree) {
        if(lqpTree instanceof SortNode || lqpTree instanceof DuplicateEliminationNode ||
                lqpTree instanceof  ProjectionNode){
            LQPNode curr = lqpTree;
            curr.setChild(optimizeLqpForSelectStatement(lqpTree.getChild()));
            return curr;
        }
        if(lqpTree instanceof SelectionNode){
            SelectionNode node = (SelectionNode) lqpTree;
            // search condition is null -> simply remove selection
            if(node.getCondition() == null){
                return node.getChild();
            }
            // single table case
            if(node.getChild() instanceof ScanNode){
                return node;
            }
            // else it will be a join node. Here we have to optimize

            // if it involves 2 tables, pass condition down simply
            if(node.getChild().getChildren().size() == 2){
                ((JoinNode)node.getChild()).setCondition(node.getCondition());
                optimizeJoinNode((JoinNode) node.getChild());
                return node.getChild();
            } else if (node.getChild().getChildren().size() == 3){
                JoinNode newJoin = new JoinNode();
                TreeNode third = removeAndGet(node.getCondition());
                newJoin.setCondition(third);
                newJoin.addChild(node.getChild().getChildren().get(1));
                newJoin.addChild(node.getChild().getChildren().get(2));
                LQPNode other = node.getChild().getChildren().get(0);

                ArrayList<LQPNode> chn = new ArrayList<>();
                chn.add(newJoin);
                chn.add(other);
                node.getChild().setChildren(chn);
                ((JoinNode)node.getChild()).setCondition(node.getCondition());

                return node.getChild();
            }
            return node;
        }
        return lqpTree;
    }

    private TreeNode removeAndGet(TreeNode node){
        TreeNode booleanTerm = node.getChildren().get(0);
        TreeNode third = booleanTerm.getChildren().get(4);

        TreeNode root = new TreeNode("search-condition", false);
        TreeNode next = new TreeNode("boolean-term", false);
        next.addChild(third);
        root.addChild(next);
        booleanTerm.removeLastChild();
        booleanTerm.removeLastChild();
        return root;
    }

    private void optimizeJoinNode(JoinNode node){
        TreeNode condition = node.getCondition();

        String rel1 = ((ScanNode)node.getChildren().get(0)).execute();
        String rel2 = ((ScanNode)node.getChildren().get(1)).execute();
        if(condition.getChildren().size() == 1){
            TreeNode main = new TreeNode("search-condition", false);
            TreeNode mainNext = new TreeNode("boolean-term", false);
            main.addChild(mainNext);
            TreeNode booleanTerm = condition.getChildren().get(0);
            List<TreeNode> booleanFactors = booleanTerm.getChildren();

            int count = 0;
            for(int i=0; i < booleanFactors.size(); i+=2){
                TreeNode boolFac = booleanFactors.get(i);
                if(tablesInvolved(boolFac).equals(rel1)){
                    TreeNode top = new TreeNode("select-statement", false);
                    TreeNode root = new TreeNode("search-condition", false);
                    top.addChild(root);
                    TreeNode next = new TreeNode("boolean-term", false);
                    root.addChild(next);
                    next.addChild(modified(boolFac));
                    SelectionNode s1 = new SelectionNode(top);
                    s1.setChild(node.getChildren().get(0));
                    node.setChild(0, s1);
                }else if(tablesInvolved(boolFac).equals(rel2)){
                    TreeNode top = new TreeNode("select-statement", false);
                    TreeNode root = new TreeNode("search-condition", false);
                    top.addChild(root);
                    TreeNode next = new TreeNode("boolean-term", false);
                    root.addChild(next);
                    next.addChild(modified(boolFac));
                    SelectionNode s1 = new SelectionNode(top);
                    s1.setChild(node.getChildren().get(1));
                    node.setChild(1, s1);
                }else{
                    if(count != 0){
                        mainNext.addChild(new TreeNode("AND", true));
                    }
                    mainNext.addChild(boolFac);
                    count++;
                }
            }

            if(mainNext.getChildren().size() == 0){
                node.setCondition(null);
            }else{
                node.setCondition(main);
            }
        }
    }

    private TreeNode modified(TreeNode node){
        TreeNode expr1 = node.getChildren().get(0);
        TreeNode expr2 = node.getChildren().get(2);

        for(TreeNode term: expr1.getChildren()){
            if(term.getValue().equals("term") && term.getChildren().get(0).getValue().equals("column-name")){
                TreeNode attrNode = term.getChildren().get(0).getChildren().get(1);
                term.getChildren().get(0).removeChildren();
                term.getChildren().get(0).addChild(attrNode);
            }
        }

        for(TreeNode term: expr2.getChildren()){
            if(term.getValue().equals("term") && term.getChildren().get(0).getValue().equals("column-name")){
                TreeNode attrNode = term.getChildren().get(0).getChildren().get(1);
                term.getChildren().get(0).removeChildren();
                term.getChildren().get(0).addChild(attrNode);
            }
        }
        return node;
    }

    private String tablesInvolved(TreeNode boolFac){
        HashSet<String> tablesInvolved = new HashSet<>();
        TreeNode expr1 = boolFac.getChildren().get(0);
        TreeNode expr2 = boolFac.getChildren().get(2);

        for(int j=0; j < expr1.getChildren().size(); j++){
            if(expr1.getChildren().get(0).getValue().equals("term") &&
                    expr1.getChildren().get(0).getChildren().get(0).getValue().equals("column-name")){
                String tableInv =
                        expr1.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue();
                tablesInvolved.add(tableInv);
            }
        }

        for(int j=0; j < expr2.getChildren().size(); j++){
            if(expr2.getChildren().get(0).getValue().equals("term") &&
                    expr2.getChildren().get(0).getChildren().get(0).getValue().equals("column-name")){
                String tableInv =
                        expr2.getChildren().get(0).getChildren().get(0).getChildren().get(0).getChildren().get(0).getValue();
                tablesInvolved.add(tableInv);
            }
        }

        List<String> t1 = new ArrayList<>(tablesInvolved);
        if(t1.size() == 1){
            return t1.get(0);
        }
        return "";
    }

    public LQPNode getLqpTree() {
        return lqpTree;
    }
}
