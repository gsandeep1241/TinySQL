package test;

import main.parsetree.*;
import org.junit.jupiter.api.Test;

import static main.utils.TreeUtils.DFSPrintTree;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import main.parser.LQPGenerator;
import main.parser.QueryParser;


public class LQPGeneratorTest {
    @Test
    void testLqpForSelectStatementBasic() {
        QueryParser qp = new QueryParser();
        TreeNode treeNode = qp.parse("SELECT a, b FROM table1;");

        // DFSPrintTree(treeNode);  // uncomment to view tree

        LQPGenerator lqpGenerator = new LQPGenerator(treeNode);

        lqpGenerator.generateLqp();

        ProjectionNode projectionNode = (ProjectionNode) lqpGenerator.getLqpTree();
        SelectionNode selectionNode = (SelectionNode) projectionNode.getChild();
        ScanNode scanNode = (ScanNode) selectionNode.getChild();

        assertEquals("∏(a,b)", projectionNode.toString(), "project parse failed.");
        assertEquals("σ()", selectionNode.toString(), "select parse failed.");
        assertEquals("⊟(table1)", scanNode.toString(), "scan parse failed.");
    }

    @Test
    void testLqpForSelectStatementWithAll() {
        QueryParser qp = new QueryParser();
        TreeNode treeNode = qp.parse("SELECT * FROM table;");

        // DFSPrintTree(treeNode);  // uncomment to view tree

        LQPGenerator lqpGenerator = new LQPGenerator(treeNode);

        lqpGenerator.generateLqp();

        ProjectionNode lqpTree = (ProjectionNode) lqpGenerator.getLqpTree();

        assertEquals(true, lqpTree.getIsAll());
        assertEquals("∏() ALL", lqpTree.toString(), "select * parse failed.");
    }

    @Test
    void testLqpForSelectStatementWithDistinct() {
        QueryParser qp = new QueryParser();
        TreeNode treeNode = qp.parse("SELECT DISTINCT a, b FROM table;");

        // DFSPrintTree(treeNode);  // uncomment to view tree

        LQPGenerator lqpGenerator = new LQPGenerator(treeNode);

        lqpGenerator.generateLqp();

        LQPNode lqpTree = lqpGenerator.getLqpTree();

        assert (lqpTree instanceof DuplicateEliminationNode);
        DuplicateEliminationNode duplicateEliminationNode = (DuplicateEliminationNode) lqpTree;
        assert (duplicateEliminationNode.getEliminationEnabled());

        assert (duplicateEliminationNode.getChild() instanceof ProjectionNode);
        ProjectionNode projectionNode = (ProjectionNode) duplicateEliminationNode.getChild();
        assertFalse(projectionNode.getIsAll());

        assertEquals("δ(∏(a,b)) ACTIVE", lqpTree.toString(), "select distinct parse failed.");
    }

    @Test
    void testLqpForSelectStatementWithCondition() {
        QueryParser qp = new QueryParser();
        TreeNode treeNode = qp.parse("SELECT a, b FROM table1, table2 WHERE a = 12 AND b = 15;");

        LQPGenerator lqpGenerator = new LQPGenerator(treeNode);

        lqpGenerator.generateLqp();

        ProjectionNode lqpTree = (ProjectionNode) lqpGenerator.getLqpTree();
        SelectionNode selectionNode = (SelectionNode) lqpTree.getChild();

        assertEquals("σ(NODE: search-condition (1 children))", selectionNode.toString());
    }

    // TODO: FROM MULTIPLE TABLES
    // TODO: ORDER BY
}
