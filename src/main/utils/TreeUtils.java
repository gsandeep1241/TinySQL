package main.utils;

import main.parsetree.JoinNode;
import main.parsetree.LQPNode;
import main.parsetree.ScanNode;
import main.parsetree.TreeNode;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

public class TreeUtils {
    public static void BFSPrintTree(TreeNode root) {
        if (root == null) {
            return;
        }

        Queue<TreeNode> queue = new LinkedList<>();
        queue.add(root);
        int total = 1;

        while (!queue.isEmpty()) {
            int size = total;
            total = 0;

            for (int i = 0; i < size; i++) {
                TreeNode node = queue.remove();
                System.out.print("\"" + node.getValue() + "\"");
                if (i != size - 1) {
                    System.out.print(", ");
                }
                List<TreeNode> children = node.getChildren();
                for (TreeNode tn : children) {
                    queue.add(tn);
                    total++;
                }
            }
            System.out.println();
        }
    }

    public static void DFSPrintTree(TreeNode node, int level) {
        if (node == null) {
            return;
        }

        System.out.println(
                (level == 0 ? "" : String.format("%" + (level * 4 + 1) + "s", "")) + node.getValue()
        );

        for (TreeNode tn : node.getChildren()) {
            TreeUtils.DFSPrintTree(tn, level + 1);
        }
    }

    public static void DFSPrintTree(TreeNode root) {
        TreeUtils.DFSPrintTree(root, 0);
    }

    public static ArrayList<String> convertSelectListToArray(TreeNode selectList) {
        ArrayList<String> columnNames = new ArrayList<>();
        for (TreeNode tn : selectList.getChildren()) {
            // select-list/column-name/attribute-name
            // TODO: doesn't support tablename.attribute-name
            if (tn.getValue().equals("column-name")) {
                columnNames.add(tn.getChildren().get(0).getChildren().get(0).getValue());
            }
        }
        return columnNames;
    }

    public static LQPNode convertTableListToJoinsAndScans(TreeNode tableList) {
        List<TreeNode> tableNames = tableList.getChildren().stream()
                .filter(tn -> !",".equals(tn.getValue()))
                .collect(Collectors.toList());

        if (tableNames.size() == 0) {
            System.out.println("CANNOT CONSTRUCT LQP TREE JOINS: INVALID PARSE TREE!");
            assert false;
            return null;
        }

        return TreeUtils.convertTableNamesToJoinsAndScans(tableNames);
    }

    public static LQPNode convertTableNamesToJoinsAndScans(List<TreeNode> tableNames) {
        // table-list / table-name / actual-table-name
        ScanNode tableLeft = new ScanNode(tableNames.get(0).getChildren().get(0).getValue());

        if (tableNames.size() == 1) {
            return tableLeft;
        }

        JoinNode joinNode = new JoinNode();
        joinNode.addChild(tableLeft);

        if (tableNames.size() > 2) {
            for(int i=1; i < tableNames.size(); i++){
                joinNode.addChild(new ScanNode(tableNames.get(i).getChildren().get(0).getValue()));
            }
        } else {
            // table-list / table-name / actual-table-name
            ScanNode tableRight = new ScanNode(tableNames.get(1).getChildren().get(0).getValue());
            joinNode.addChild(tableRight);
        }

        return joinNode;
    }
}
