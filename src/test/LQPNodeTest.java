package test;

import main.parser.QueryParser;
import main.parsetree.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LQPNodeTest {
    @Test
    void testProjectionNode() {
        QueryParser qp = new QueryParser();
        ProjectionNode projection;

        projection = new ProjectionNode(qp.parse("SELECT * FROM table1;"));
        assert(projection.getIsAll());

        projection = new ProjectionNode(qp.parse("SELECT a FROM table1;"));
        assert(!projection.getIsAll());
        assertEquals("[a]", projection.getColumns().toString());

        projection = new ProjectionNode(qp.parse("SELECT a,b FROM table1;"));
        assert(!projection.getIsAll());
        assertEquals("[a, b]", projection.getColumns().toString());

        // TODO Add tests for execute() -- should mock getChild() method
    }

    @Test
    void testDuplicateEliminationNode() {
        QueryParser qp = new QueryParser();
        DuplicateEliminationNode duplicateEliminationNode;

        duplicateEliminationNode = new DuplicateEliminationNode(qp.parse("SELECT * FROM table1;"));
        assert(!duplicateEliminationNode.getEliminationEnabled());

        duplicateEliminationNode = new DuplicateEliminationNode(qp.parse("SELECT DISTINCT * FROM table1;"));
        assert(duplicateEliminationNode.getEliminationEnabled());

        duplicateEliminationNode = new DuplicateEliminationNode(false);
        assert(!duplicateEliminationNode.getEliminationEnabled());

        duplicateEliminationNode = new DuplicateEliminationNode(true);
        assert(duplicateEliminationNode.getEliminationEnabled());

        // TODO Add tests for execute() -- should mock getChild() method
    }

    @Test
    void testSortNode() {
        QueryParser qp = new QueryParser();
        SortNode sortNode;

        sortNode = new SortNode(qp.parse("SELECT * FROM table1;"));
        assert(!sortNode.getSortingEnabled());
        assertNull(sortNode.getColumnName());

        sortNode = new SortNode(qp.parse("SELECT * FROM table1 ORDER BY column1;"));
        assert(sortNode.getSortingEnabled());
        assertEquals("column1", sortNode.getColumnName());

        // TODO Add tests for execute() -- should mock getChild() method
    }

    @Test
    void testSelectionNode() {
        QueryParser qp = new QueryParser();
        SelectionNode selectionNode;

        selectionNode = new SelectionNode(qp.parse("SELECT * FROM table1;"));
        assertNull(selectionNode.getCondition());

        selectionNode = new SelectionNode(qp.parse("SELECT * FROM table1 WHERE a.id > b.id;"));
        assertNotNull(selectionNode.getCondition());

        // TODO Add tests for parsed boolean expression
        // TODO Add tests for execute() -- should mock getChild() method
    }

    @Test
    void testScanNode() {
        QueryParser qp = new QueryParser();
        ScanNode scanNode;

        scanNode = new ScanNode("table1");
        assertEquals("table1", scanNode.getTableName());

        // TODO Add tests for execute() -- should mock storageManager methods
    }

    @Test
    void testJoinNode() {
        // TODO Add tests for execute() -- should mock getChild() method
    }
}
