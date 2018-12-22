package test;

import main.parser.QueryParser;
import main.parsetree.JoinNode;
import main.parsetree.LQPNode;
import main.parsetree.ScanNode;
import main.parsetree.TreeNode;
import org.junit.jupiter.api.Test;
import sun.reflect.generics.tree.Tree;

import static main.utils.TreeUtils.convertTableListToJoinsAndScans;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TreeUtilsTest {
    @Test
    void testConvertTableListToJoinsAndScans() {
        QueryParser qp = new QueryParser();
        TreeNode treeNode, tableList;
        LQPNode lqpNode;
        ScanNode scanNode;
        JoinNode joinNode;

        // single table
        treeNode = qp.parse("SELECT * FROM table1;");
        tableList = treeNode.getChildren().stream()
                .filter(tn -> "table-list".equals(tn.getValue()))
                .findFirst()
                .orElse(null);
        lqpNode = convertTableListToJoinsAndScans(tableList);
        assert(lqpNode instanceof ScanNode);
        scanNode = (ScanNode) lqpNode;
        assertEquals("table1", scanNode.getTableName());

        // two tables
        treeNode = qp.parse("SELECT * FROM table1, table2;");
        tableList = treeNode.getChildren().stream()
                .filter(tn -> "table-list".equals(tn.getValue()))
                .findFirst()
                .orElse(null);
        lqpNode = convertTableListToJoinsAndScans(tableList);
        assert(lqpNode instanceof JoinNode);
        joinNode = (JoinNode) lqpNode;
        assert(joinNode.getChildren().get(0) instanceof ScanNode);
        assert(joinNode.getChildren().get(1) instanceof ScanNode);
        scanNode = (ScanNode) joinNode.getChildren().get(0);
        assertEquals("table1", scanNode.getTableName());
        scanNode = (ScanNode) joinNode.getChildren().get(1);
        assertEquals("table2", scanNode.getTableName());
    }

    @Test
    void testConvertTableListToJoinsAndScans_3Tables() {
        QueryParser qp = new QueryParser();
        TreeNode treeNode, tableList;
        LQPNode lqpNode;
        ScanNode scanNode;
        JoinNode joinNode;

        // three tables
        treeNode = qp.parse("SELECT * FROM table1, table2, table3;");
        tableList = treeNode.getChildren().stream()
                .filter(tn -> "table-list".equals(tn.getValue()))
                .findFirst()
                .orElse(null);

        lqpNode = convertTableListToJoinsAndScans(tableList);
        assert(lqpNode instanceof JoinNode);
        joinNode = (JoinNode) lqpNode;
        assert(joinNode.getChildren().get(0) instanceof ScanNode);
        assert(joinNode.getChildren().get(1) instanceof ScanNode);
        assert(joinNode.getChildren().get(2) instanceof ScanNode);
        assertEquals("table1", ((ScanNode)(joinNode.getChildren().get(0))).getTableName());
        assertEquals("table2", ((ScanNode)(joinNode.getChildren().get(1))).getTableName());
        assertEquals("table3", ((ScanNode)(joinNode.getChildren().get(2))).getTableName());
    }
}
