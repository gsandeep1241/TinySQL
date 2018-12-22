package test;

import main.parser.QueryParser;
import main.parsetree.TreeNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import static main.utils.TreeUtils.DFSPrintTree;

public class BatchQueryParserTest {
    ArrayList<String> failedQueries = new ArrayList<>();

    @Test
    void testParseQueriesInBatch() {
        String fileName = "resources/test_queries.sql";

        Path path = Paths.get(fileName);
        try {
            Files.lines(path).forEach(this::parseQuery);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("FAILED QUERIES");
        System.out.println(this.failedQueries);
        System.out.println(this.failedQueries.size());
    }


    private void parseQuery(String query) {
        if (!query.endsWith(";")) {
            query = query + ";";
        }
        QueryParser qp = new QueryParser();
        TreeNode treeNode = qp.parse(query);
        if (treeNode ==  null) {
            failedQueries.add(query);
            // System.out.println("Parse failed for - " + query);
            // assert false;
        } else {
            System.out.println("Parse success.");
            // may add various assertions here.
        }
    }
}
