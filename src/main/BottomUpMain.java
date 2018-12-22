package main;

import main.parser.QueryParser;
import main.parsetree.TreeNode;
import main.utils.TreeUtils;

public class BottomUpMain {

    public static void main(String[] args) {
        System.out.println("Welcome to TinySQL!");

        QueryParser parser = new QueryParser();

        String createSql = "CREATE TABLE teams(id INT, name STR20, type INT);";
        TreeNode tn1 = parser.parse(createSql);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn1);
        System.out.println();

        String dropSql = "DROP TABLE teams;";
        TreeNode tn2 = parser.parse(dropSql);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn2);
        System.out.println();

        String simpleSelectSql1 = "SELECT * FROM teams;";
        TreeNode tn3 = parser.parse(simpleSelectSql1);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn3);
        System.out.println();

        String simpleSelectSql2 = "SELECT teams.id, name FROM teams;";
        TreeNode tn4 = parser.parse(simpleSelectSql2);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.DFSPrintTree(tn4);
        System.out.println();

        String simpleSelectSql3 = "SELECT id FROM teams;";
        TreeNode tn5 = parser.parse(simpleSelectSql3);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn5);
        System.out.println();

        String deleteSql = "DELETE FROM teams;";
        TreeNode tn6 = parser.parse(deleteSql);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn6);
        System.out.println();

        String deleteComplexSql1 = "DELETE FROM teams WHERE id > 1 AND name = 'hello' OR name = 'temp';";
        TreeNode tn7 = parser.parse(deleteComplexSql1);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn7);
        System.out.println();

        String deleteComplexSql2 = "DELETE FROM teams WHERE id > 1 AND name = hello OR name = teams.val;";
        TreeNode tn8 = parser.parse(deleteComplexSql2);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn8);
        System.out.println();

        String complexSelectSql1 = "SELECT * FROM teams WHERE id > 1 AND name = 'hello';";
        TreeNode tn9 = parser.parse(complexSelectSql1);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn9);
        System.out.println();

        String complexSelectSql2 = "SELECT id FROM teams WHERE id > (1 + 8) AND name = 'hello';";
        TreeNode tn10 = parser.parse(complexSelectSql2);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn10);
        System.out.println();

        String complexSelectSql3 = "SELECT * FROM teams WHERE id > 1 AND name = hello ORDER BY teams.name;";
        TreeNode tn11 = parser.parse(complexSelectSql3);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn11);
        System.out.println();

        String complexSelectSql4 = "SELECT id, teams FROM teams ORDER BY name;";
        TreeNode tn12 = parser.parse(complexSelectSql4);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn12);
        System.out.println();

        String simpleInsertSql1 = "INSERT INTO teams(id, name) VALUES(1, NULL);";
        TreeNode tn13 = parser.parse(simpleInsertSql1);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn13);
        System.out.println();

        String simpleInsertSql2 = "INSERT INTO teams(id) SELECT id FROM teams;";
        TreeNode tn14 = parser.parse(simpleInsertSql2);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn14);
        System.out.println();

        String simpleInsertSql3 = "INSERT INTO teams (id, name, something) VALUES (1, NULL, \"some value\");";
        TreeNode tn15 = parser.parse(simpleInsertSql3);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn15);
        System.out.println();

        String compSel = "SELECT * FROM course WHERE (exam + homework) = 200;";
        TreeNode tn16 = parser.parse(compSel);
        System.out.println();

        System.out.println("Printing tree: ");
        TreeUtils.BFSPrintTree(tn16);
        System.out.println();
    }
}


