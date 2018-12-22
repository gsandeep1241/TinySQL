package test;

import main.parser.QueryParser;
import main.parsetree.TreeNode;
import org.junit.jupiter.api.Test;

import static main.utils.TreeUtils.DFSPrintTree;

public class QueryParserTest {
    @Test
    void testQueryParserGeneratedTree() {
        QueryParser qp = new QueryParser();
        TreeNode treeNode = qp.parse("SELECT DISTINCT a, b FROM table1, table2, table3 WHERE a = 12 AND b = 15 ORDER BY a;");

        DFSPrintTree(treeNode);
    }
}
