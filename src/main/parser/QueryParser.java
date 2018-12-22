package main.parser;

import main.parsetree.TreeNode;

public class QueryParser {

    private class IntWrapper {
        int val;
    }

    public QueryParser(){
        print("Initializing QueryParser..");
    }

    public TreeNode parse(String sql){
        print("Parsing query: " + sql);
        return parseStatement(sql);
    }

    private TreeNode parseStatement(String sql){
        int l = sql.length();
        int i = 0;
        while(i < l && sql.charAt(i) != ' '){
            i++;
        }
        if(i == l){
            print("Error parsing statement: Only one word in the statement.");
            return null;
        }

        String type = sql.substring(0, i);
        TreeNode result;
        if(type.equals("CREATE")){
            result = parseCreateTableStatement(sql);
        }else if(type.equals("DROP")){
            result = parseDropTableStatement(sql);
        }else if(type.equals("SELECT")){
            result = parseSelectTableStatement(sql);
        }else if(type.equals("DELETE")){
            result = parseDeleteTableStatement(sql);
        }else if(type.equals("INSERT")){
            result = parseInsertTableStatement(sql);
        }else{
            print("Incorrect token found: " + type);
            return null;
        }
        if(result != null){
            print("Parsing statement successful.");
        }
        return result;
    }

    private TreeNode parseCreateTableStatement(String sql){
        print("Parsing create table statement..");
        TreeNode root = new TreeNode("create-table-statement", false);
        root.addChild(new TreeNode("CREATE", true));

        int l = sql.length();
        if(sql.substring(7, 12).equals("TABLE")){
            root.addChild(new TreeNode("TABLE", true));
        }else{
            int i = 7;
            while(i < l && sql.charAt(i) != ' '){
                i++;
            }
            print("Incorrect token found: " + sql.substring(7, i));
            return null;
        }

        IntWrapper iw = new IntWrapper();
        iw.val = 13;
        TreeNode tableNameNode = parseTableNameInSql(sql, iw);
        if(tableNameNode == null){
            return null;
        }
        root.addChild(tableNameNode);
        int i = iw.val;
        while(sql.charAt(i) == ' '){
            i++;
        }
        iw.val = i;

        if(i == l || sql.charAt(i) != '('){
            print("Incorrect SQL: Expecting ( character at pos: " + i);
            return null;
        }
        root.addChild(new TreeNode("(", true));
        i++;
        int start = i;
        while(i < l && sql.charAt(i) != ')'){
            i++;
        }
        if(i == l){
            print("Incorrect SQL: ) character not found.");
            return null;
        }
        TreeNode attrTypeListNode = parseAttrTypeList(sql.substring(start, i));
        if(attrTypeListNode == null){
            return null;
        }
        root.addChild(attrTypeListNode);
        root.addChild(new TreeNode(")", true));
        i++;
        if(i == l-1 && sql.charAt(i) == ';'){
            return root;
        }
        print("Incorrect SQL: Only semicolon expected after attribute type list.");
        return null;
    }

    private TreeNode parseDropTableStatement(String sql){
        print("Parsing drop table statement..");
        TreeNode root = new TreeNode("drop-table-statement", false);
        root.addChild(new TreeNode("DROP", true));

        int l = sql.length();
        if(sql.substring(5, 10).equals("TABLE")){
            root.addChild(new TreeNode("TABLE", true));
        }else{
            int i = 5;
            while(i < l && sql.charAt(i) != ' '){
                i++;
            }
            print("Incorrect token found: " + sql.substring(5, i));
            return null;
        }
        IntWrapper iw = new IntWrapper();
        iw.val = 11;
        TreeNode tableNameNode = parseTableNameInSql(sql, iw);
        if(tableNameNode == null){
            return null;
        }
        root.addChild(tableNameNode);
        int i = iw.val;
        if(i == l-1 && sql.charAt(i) == ';'){
            return root;
        }
        print("Incorrect SQL: Only semicolon expected after table name.");
        return null;
    }

    private TreeNode parseSelectTableStatement(String sql){
        print("Parsing select statement..");
        TreeNode root = new TreeNode("select-statement", false);
        root.addChild(new TreeNode("SELECT", true));

        int l = sql.length();
        int i = 7;
        if(sql.substring(7, 16).equals("DISTINCT ")){
            root.addChild(new TreeNode("DISTINCT", true));
            i = 16;
        }
        int start = i;

        while(i < l && sql.charAt(i) != 'F'){
            i++;
        }
        if(i == l){
            print("Incorrect SQL: FROM not found.");
            return null;
        }
        TreeNode selectList = parseSelectList(sql.substring(start, i));
        if(selectList == null){
            return null;
        }
        root.addChild(selectList);
        if(!sql.substring(i, i+5).equals("FROM ")){
            print("Incorrect SQL: FROM keyword incorrect.");
            return null;
        }
        root.addChild(new TreeNode("FROM", true));

        i += 5;
        start = i;
        while(i < l){
            if(sql.charAt(i) == 'W' || sql.charAt(i) == 'O' || sql.charAt(i) == ';'){
                break;
            }
            i++;
        }
        if(i == l){
            print("Incorrect SQL: unexpected end of string.");
            return null;
        }
        TreeNode tableList = parseTableList(sql.substring(start, i));
        if(tableList == null){
            return null;
        }
        root.addChild(tableList);

        if(sql.charAt(i) == 'W'){
            start = i;
            while(i < l){
                if( (sql.charAt(i) == 'O' && sql.charAt(i+1) == 'R' && sql.charAt(i+2) == 'D') || sql.charAt(i) == ';'){
                    break;
                }
                i++;
            }
            if(i == l){
                print("Invalid where clause string.");
                return null;
            }
            int j = l-1;
            boolean orderSection = false;
            if(sql.charAt(i) == ';' && i == l-1){
                j = l-1;
            }else if(sql.charAt(i) == ';' && i != l-1){
                print("Incorrect semi colon found.");
                return null;
            }else{
                j = i-1;
                orderSection = true;
            }

            if(sql.substring(start, start+6).equals("WHERE ")){
                root.addChild(new TreeNode("WHERE", true));
            }else{
                print("WHERE not found.");
                return null;
            }

            i = start + 6;
            TreeNode searchCondNode = parseSearchCondition(sql.substring(i, j));
            if(searchCondNode == null){
                return null;
            }
            root.addChild(searchCondNode);

            if(!orderSection){
                return root;
            }else{
                if(sql.charAt(l-1) != ';'){
                    print("Should end with semi colon.");
                    return null;
                }
                j++;
                if(!sql.substring(j, j + 9).equals("ORDER BY ")){
                    print("Missing keyword ORDER BY.");
                    return null;
                }
                root.addChild(new TreeNode("ORDER BY", true));
                j += 9;
                if(checkColNameAndAdd(sql.substring(j, l-1), root)){
                    return root;
                }else{
                    print("Column name not correctly formatted in ORDER BY.");
                    return null;
                }
            }
        }else if(sql.charAt(i) == 'O'){
            if(sql.charAt(l-1) != ';'){
                print("Should end with semi colon.");
                return null;
            }
            if(!sql.substring(i, i + 9).equals("ORDER BY ")){
                print("Missing keyword ORDER BY.");
                return null;
            }
            root.addChild(new TreeNode("ORDER BY", true));
            i += 9;
            if(checkColNameAndAdd(sql.substring(i, l-1), root)){
                return root;
            }else{
                print("Column name not correctly formatted in ORDER BY.");
                return null;
            }
        }else if(sql.charAt(i) == ';'){
            if(i != l-1){
                print("Unexpected semicolon found");
                return null;
            }else{
                return root;
            }
        }
        return null;
    }

    private TreeNode parseDeleteTableStatement(String sql){
        print("Parsing delete table statement..");
        TreeNode root = new TreeNode("delete-table-statement", false);
        root.addChild(new TreeNode("DELETE", true));

        int l = sql.length();
        if(sql.substring(7, 11).equals("FROM")){
            root.addChild(new TreeNode("FROM", true));
        }else{
            int i = 7;
            while(i < l && sql.charAt(i) != ' '){
                i++;
            }
            print("Incorrect token found: " + sql.substring(7, i));
            return null;
        }
        IntWrapper iw = new IntWrapper();
        iw.val = 12;
        TreeNode tableNameNode = parseTableNameInSql(sql, iw);
        if(tableNameNode == null){
            return null;
        }
        root.addChild(tableNameNode);
        int i = iw.val;
        if(i == l-1 && sql.charAt(i) == ';'){
            return root;
        }else if(i < l-1 && sql.charAt(i+1) == 'W'){
            if(sql.substring(i+1, i+7).equals("WHERE ")){
                root.addChild(new TreeNode("WHERE", true));
                if(sql.charAt(l-1) != ';'){
                    print("Expecting semicolon at the end.");
                    return null;
                }
                String substr = sql.substring(i+7);
                substr = substr.substring(0, substr.length()-1);
                TreeNode searchCondNode = parseSearchCondition(substr);
                if(searchCondNode == null){
                    return null;
                }
                root.addChild(searchCondNode);
                return root;
            }else{
                print("Incorrect SQL: WHERE expected.");
                return null;
            }
        }
        print("Incorrect SQL: WHERE or semicolon expected after table name.");
        return null;
    }

    private TreeNode parseInsertTableStatement(String sql){
        print("Parsing insert table statement..");
        TreeNode root = new TreeNode("insert-table-statement", false);
        root.addChild(new TreeNode("INSERT", true));

        int l = sql.length();
        if(sql.substring(7, 11).equals("INTO")){
            root.addChild(new TreeNode("INTO", true));
        }else{
            int i = 7;
            while(i < l && sql.charAt(i) != ' '){
                i++;
            }
            print("Incorrect token found: " + sql.substring(7, i));
            return null;
        }

        IntWrapper iw = new IntWrapper();
        iw.val = 12;
        TreeNode tableNameNode = parseTableNameInSql(sql, iw);
        if(tableNameNode == null){
            return null;
        }
        root.addChild(tableNameNode);
        int i = iw.val;

        while(sql.charAt(i) == ' '){
            i++;
        }
        iw.val = i;

        if(i == l || sql.charAt(i) != '('){
            print("Incorrect SQL: Expecting ( character at pos: " + i);
            return null;
        }
        root.addChild(new TreeNode("(", true));
        i++;
        int start = i;
        while(i < l && sql.charAt(i) != ')'){
            i++;
        }
        if(i == l){
            print("Incorrect SQL: ) character not found.");
            return null;
        }
        TreeNode attrList = parseAttrList(sql.substring(start, i));
        if(attrList == null){
            return null;
        }
        root.addChild(attrList);
        root.addChild(new TreeNode(")", true));
        i++;
        if(sql.charAt(i) != ' '){
            print("Error: space not found after closing braces.");
            return null;
        }
        i++;
        if(sql.charAt(l-1) != ';'){
            print("Expecting semi colon at the end.");
            return null;
        }
        TreeNode insertTuplesNode = parseInsertTuples(sql.substring(i, l-1));
        if(insertTuplesNode == null){
            return null;
        }
        root.addChild(insertTuplesNode);
        return root;
    }

    private TreeNode parseTableNameInSql(String str, IntWrapper iw){
        TreeNode root = new TreeNode("table-name", false);
        int l = str.length();
        int start = iw.val;
        while(iw.val < l && str.charAt(iw.val) != ' '){
            if(iw.val == start && isDigit(str.charAt(iw.val))){
                print("Incorrect SQL: First character of table name cannot be digit.");
                return null;
            }
            if(isLetter(str.charAt(iw.val)) || isDigit(str.charAt(iw.val))){
                iw.val++;
            }else{
                break;
            }
        }

        root.addChild(new TreeNode(str.substring(start, iw.val), true));
        return root;
    }

    private TreeNode parseAttrTypeList(String attrTypeList){
        print("Parsing attribute type list.. " + attrTypeList);
        TreeNode root = new TreeNode("attribute-type-list", false);
        String[] split = attrTypeList.split(",");
        int num = split.length;

        if(num == 0){
            print("Incorrect SQL: Empty attribute type list.");
            return null;
        }

        for(int i=0; i < num; i++){
            if(split[i].length() == 0){
                print("Incorrect attribute type list.");
                return null;
            }
            String str = (split[i].charAt(0) == ' ') ? split[i].substring(1) : split[i];
            str = str.trim();
            String[] attrsAndType = str.split(" ");
            if(attrsAndType.length != 2){
                print("Individual attribute-type pair is incorrect.");
                return null;
            }
            if(i != 0){
                root.addChild(new TreeNode(",", true));
            }
            if(checkAttributeAndAdd(attrsAndType[0], root) && checkTypeAndAdd(attrsAndType[1], root)){
                continue;
            }
            print("Incorrect SQL: Attribute name or type is incorrect.");
            return null;
        }
        return root;
    }

    private TreeNode parseAttrList(String str){
        print("Parsing attribute list.. " + str);
        TreeNode root = new TreeNode("attribute-list", false);
        if(str.length() == 0){
            print("Attribute list cannot be empty.");
            return null;
        }

        String[] split = str.split(",");
        int num = split.length;
        if(num == 0){
            print("Attribute list is empty.");
            return null;
        }
        for(int i=0; i < num; i++){
            if(split[i].length() == 0){
                print("Incorrect attribute name.");
                return null;
            }
            String spl = (split[i].charAt(0) == ' ') ? split[i].substring(1) : split[i];
            spl = spl.trim();
            if(i != 0){
                root.addChild(new TreeNode(",", true));
            }
            if(checkAttributeAndAdd(spl, root)){
                continue;
            }
            print("Incorrect SQL: Attribute name is incorrect.");
            return null;
        }
        return root;
    }

    private TreeNode parseInsertTuples(String str){
        print("Parsing insert tuples.. " + str);
        int l = str.length();
        TreeNode root = new TreeNode("insert-list", false);
        if(str.substring(0, 6).equals("SELECT")){
            TreeNode selectNode = parseSelectTableStatement(str + ';');
            if(selectNode == null){
                return null;
            }
            root.addChild(selectNode);
            return root;
        }else if(str.substring(0, 6).equals("VALUES")){
            root.addChild(new TreeNode("VALUES", true));
            int i = 6;
            while(str.charAt(i) == ' '){
                i++;
            }
            if(str.charAt(i) != '('){
                print("Expecting ( character.");
                return null;
            }
            TreeNode node1 = new TreeNode("(", true);
            root.addChild(node1);
            i++;
            int begin = i;
            while(i < l && str.charAt(i) != ')'){
                i++;
            }
            int end = i;
            if(i == l){
                print("Closing braces not found.");
                return null;
            }else if(str.charAt(i) == ')'){
                if(i != l-1){
                    print("Unknown parenthesis found in insert list.");
                    return null;
                }else{
                    TreeNode valuesList = parseValuesList(str.substring(begin, end));
                    if(valuesList == null){
                        return null;
                    }
                    root.addChild(valuesList);
                }
            }
            TreeNode node2 = new TreeNode(")", true);
            root.addChild(node2);
            return root;
        }else{
            print("Error in insert-tuples. Keyword not found.");
            return null;
        }
    }

    private TreeNode parseValuesList(String str){
        print("Parsing values list.. " + str);
        TreeNode root = new TreeNode("values-list", false);
        if(str.length() == 0){
            print("Values list cannot be empty.");
            return null;
        }

        String[] split = str.split(",");
        int num = split.length;
        if(num == 0){
            print("Values list is empty.");
            return null;
        }
        for(int i=0; i < num; i++){
            if(split[i].length() == 0){
                print("Incorrect value.");
                return null;
            }
            String spl = (split[i].charAt(0) == ' ') ? split[i].substring(1) : split[i];
            spl = spl.trim();
            if(i != 0){
                root.addChild(new TreeNode(",", true));
            }
            if(checkValueAndAdd(spl, root)){
                continue;
            }
            print("Incorrect SQL: Value is incorrect.");
            return null;
        }
        return root;
    }

    private boolean checkValueAndAdd(String str, TreeNode root){
        boolean ans = false;
        TreeNode node = new TreeNode("value", false);
        if(str.equals("NULL")){
            node.addChild(new TreeNode("NULL", true));
            ans = true;
        }else if(checkIntAndAdd(str, node)){
            ans = true;
        }else if(checkLiteralAndAdd(str, node)){
            ans = true;
        }
        if(ans){
            root.addChild(node);
        }
        return ans;
    }

    private boolean checkLiteralAndAdd(String str, TreeNode root){
        print("Parsing literal.. " + str);
        TreeNode node = new TreeNode("literal", false);

        int l = str.length();
        if(l == 0){
            print("Empty literal.");
            return false;
        }

        /*if(str.contains("\"")){
            print("Literal contains double quote.");
            return false;
        };*/

        node.addChild(new TreeNode(str, true));
        root.addChild(node);
        return true;
    }

    private boolean checkAttributeAndAdd(String str, TreeNode root){
        int l = str.length();
        if(l != 0 && isDigit(str.charAt(0))){
            return false;
        }else if(l == 0){
            return false;
        }
        for(int i=1; i < l; i++){
            char c = str.charAt(i);
            if(isDigit(c) || isLetter(c)){
                continue;
            }else{
                return false;
            }
        }
        TreeNode node = new TreeNode("attribute-name", false);
        node.addChild(new TreeNode(str, true));
        root.addChild(node);
        return true;
    }

    private boolean checkTypeAndAdd(String str, TreeNode root){
        TreeNode node = new TreeNode("data-type", false);
        if(str.equals("INT") || str.equals("STR20")){
            node.addChild(new TreeNode(str, true));
            root.addChild(node);
            return true;
        }
        return false;
    }

    private TreeNode parseSelectList(String str){
        print("Parsing select list.. " + str);
        TreeNode root = new TreeNode("select-list", false);
        if(str.length() == 0){
            print("Select list cannot be empty.");
            return null;
        }
        if(str.charAt(0) == '*'){
            if(str.length() != 2 || str.charAt(1) != ' '){
                print("Star found. It should be followed by a space and FROM.");
                return null;
            }
            root.addChild(new TreeNode("*", true));
            return root;
        }

        str = str.substring(0, str.length()-1);
        String[] split = str.split(",");
        int num = split.length;
        if(num == 0){
            print("Select list is empty.");
            return null;
        }
        for(int i=0; i < num; i++){
            if(split[i].length() == 0){
                print("Incorrect select list.");
                return null;
            }
            String spl = (split[i].charAt(0) == ' ') ? split[i].substring(1) : split[i];
            spl = spl.trim();
            if(i != 0){
                root.addChild(new TreeNode(",", true));
            }
            if(checkColNameAndAdd(spl, root)){
                continue;
            }
            print("Incorrect SQL: Column name is incorrect.");
            return null;
        }
        return root;
    }

    private boolean checkColNameAndAdd(String str, TreeNode root){
        TreeNode node = new TreeNode("column-name", false);
        IntWrapper iw = new IntWrapper();
        if(str.length() == 0){
            return false;
        }
        if(str.contains(".") && str.charAt(0) != '.'){
            TreeNode tableNameNode = parseTableNameInSql(str, iw);
            if(tableNameNode == null){
                return false;
            }
            node.addChild(tableNameNode);
        }else if(str.charAt(0) == '.'){
            return false;
        }
        if(str.charAt(iw.val) == '.'){
            iw.val++;
        }

        if(checkAttributeAndAdd(str.substring(iw.val), node)){
            root.addChild(node);
            return true;
        }
        return false;
    }

    private TreeNode parseTableList(String str){
        print("Parsing table list.. " + str);
        TreeNode root = new TreeNode("table-list", false);
        if(str.length() == 0){
            print("Table list cannot be empty.");
            return null;
        }

        if(str.charAt(str.length()-1) == ' '){
            str = str.substring(0, str.length()-1);
        }
        String[] split = str.split(",");
        int num = split.length;
        if(num == 0){
            print("Table list is empty.");
            return null;
        }
        for(int i=0; i < num; i++){
            if(split[i].length() == 0){
                print("Incorrect table list.");
                return null;
            }
            String spl = (split[i].charAt(0) == ' ') ? split[i].substring(1) : split[i];
            spl = spl.trim();
            if(i != 0){
                root.addChild(new TreeNode(",", true));
            }
            IntWrapper iw = new IntWrapper();
            TreeNode tableNameNode = parseTableNameInSql(spl, iw);
            if(tableNameNode == null){
                return null;
            }
            if(iw.val == spl.length()){
                root.addChild(tableNameNode);
                continue;
            }
            print("Incorrect SQL: Table name is incorrect.");
            return null;
        }
        return root;
    }

    private TreeNode parseSearchCondition(String str){
        print("Parsing search condition.. " + str);
        TreeNode root = new TreeNode("search-condition", false);
        int l = str.length();
        if(l == 0){
            print("Empty search condition.");
            return null;
        }

        String[] split = str.split("OR");
        int num = split.length;
        for(int i=0; i < num; i++){
            if(split[i].length() == 0){
                print("Incorrect boolean term.");
                return null;
            }
            String spl = (split[i].charAt(0) == ' ') ? split[i].substring(1) : split[i];
            spl = spl.trim();
            if(i != 0){
                root.addChild(new TreeNode("OR", true));
            }

            TreeNode booleanTermNode = parseBooleanTerm(spl);
            if(booleanTermNode == null){
                return null;
            }
            root.addChild(booleanTermNode);
        }
        return root;
    }

    private TreeNode parseBooleanTerm(String str){
        print("Parsing boolean term.. " + str);
        TreeNode root = new TreeNode("boolean-term", false);
        int l = str.length();
        if(l == 0){
            print("Empty boolean term.");
            return null;
        }

        String[] split = str.split("AND");
        int num = split.length;
        for(int i=0; i < num; i++){
            if(split[i].length() == 0){
                print("Incorrect boolean factor.");
                return null;
            }
            String spl = (split[i].charAt(0) == ' ') ? split[i].substring(1) : split[i];
            spl = spl.trim();
            if(i != 0){
                root.addChild(new TreeNode("AND", true));
            }

            TreeNode expressionNode = parseBooleanFactor(spl);
            if(expressionNode == null){
                return null;
            }
            root.addChild(expressionNode);
        }
        return root;
    }

    private TreeNode parseBooleanFactor(String str){
        print("Parsing boolean factor.. " + str);
        TreeNode root = new TreeNode("boolean-factor", false);
        int l = str.length();

        if(l == 0){
            print("Incorrect boolean factor.");
            return null;
        }

        String compOp = "";
        int count = 0;
        if(str.contains(">")){
            compOp = ">";
            count++;
        }
        if(str.contains("<")){
            compOp = "<";
            count++;
        }
        if(str.contains("=")){
            compOp = "=";
            count++;
        }

        if(count != 1){
            print("Too many/too few comparision operators.");
            return null;
        }

        String[] split = str.split(compOp);
        if(split.length != 2){
            print("Incorrect boolean factor. Expression terms not equal to 2.");
            return null;
        }

        TreeNode expr1 = parseExpression(split[0]);
        if(expr1 == null){
            return null;
        }
        root.addChild(expr1);

        TreeNode compNode = new TreeNode("comp-op", false);
        TreeNode comp = new TreeNode(compOp, true);
        compNode.addChild(comp);
        root.addChild(compNode);

        TreeNode expr2 = parseExpression(split[1]);
        if(expr2 == null){
            return null;
        }
        root.addChild(expr2);
        return root;
    }

    private TreeNode parseExpression(String str){
        print("Parsing expression.. " + str);
        int l = str.length();
        TreeNode root = new TreeNode("expression", false);
        if(l == 0){
            print("Incorrect expression string.");
            return null;
        }

        str = str.trim();
        if(str.charAt(0) == ' '){
            str = str.substring(1);
        }

        if(str.contains("+") && str.contains("-")){
            print("Expression contains both + and -.");
            return null;
        }else if(str.contains("+")){
            if(str.charAt(0) != '(' || str.charAt(str.length()-1) != ')'){
                print("Invalid braces. Braces missing.");
                return null;
            }

            TreeNode node1 = new TreeNode("(", true);
            root.addChild(node1);

            str = str.substring(1, str.length()-1);
            String[] split = str.split("\\+");
            if(split.length != 2){
                print("Expression incorrect. Invalid length of terms.");
                return null;
            }
            TreeNode termNode1 = parseTermNode(split[0]);
            if(termNode1 == null){
                return null;
            }
            root.addChild(termNode1);
            TreeNode plusNode = new TreeNode("+", true);
            root.addChild(plusNode);
            TreeNode termNode2 = parseTermNode(split[1]);
            if(termNode2 == null){
                return null;
            }
            root.addChild(termNode2);
            TreeNode node2 = new TreeNode(")", true);
            root.addChild(node2);
        }else if(str.contains("-")){
            if(str.charAt(0) != '(' || str.charAt(str.length()-1) != ')'){
                print("Invalid braces. Braces missing.");
                return null;
            }

            TreeNode node1 = new TreeNode("(", true);
            root.addChild(node1);

            str = str.substring(1, str.length()-1);
            String[] split = str.split("-");
            if(split.length != 2){
                print("Expression incorrect. Invalid length of terms.");
                return null;
            }
            TreeNode termNode1 = parseTermNode(split[0]);
            if(termNode1 == null){
                return null;
            }
            root.addChild(termNode1);
            TreeNode minusNode = new TreeNode("-", true);
            root.addChild(minusNode);
            TreeNode termNode2 = parseTermNode(split[1]);
            if(termNode2 == null){
                return null;
            }
            root.addChild(termNode2);

            TreeNode node2 = new TreeNode("(", true);
            root.addChild(node2);
        }else{
            TreeNode termNode = parseTermNode(str);
            if(termNode == null){
                return null;
            }
            root.addChild(termNode);
        }

        return root;
    }

    private TreeNode parseTermNode(String str){
        print("Parsing term.. " + str);
        TreeNode root = new TreeNode("term", false);

        if(str.length() == 0){
            print("Term string empty.");
            return null;
        }
        str = (str.charAt(0) == ' ') ? str.substring(1) : str;
        str= (str.charAt(str.length()-1) == ' ') ? str.substring(0, str.length()-1) : str;
        int l = str.length();
        if(l == 0){
            print("Empty term.");
            return null;
        }
        if(checkColNameAndAdd(str, root)){
            return root;
        }else if(checkIntAndAdd(str, root)){
            return root;
        }else if(checkLiteralAndAdd(str, root)){
            return root;
        }
        print("Unable to parse term node.");
        return null;
    }

    private boolean checkIntAndAdd(String str, TreeNode root){
        TreeNode node = new TreeNode("integer", false);
        int l = str.length();
        if(l == 0){
            return false;
        }
        str = str.trim();
        if(str.charAt(0) == ' '){
            str = str.substring(1);
        }

        for(int i=0; i < str.length(); i++){
            if(!isDigit(str.charAt(i))){
                return false;
            }
        }
        node.addChild(new TreeNode(str, true));
        root.addChild(node);
        return true;
    }

    private boolean isLetter(char c){
        if(c >= 97 && c <= 122){
            return true;
        }
        return false;
    }

    private boolean isDigit(char c){
        if(c >= 48 && c <= 57){
            return true;
        }
        return false;
    }

    private void print(String s){
        // System.out.println(s);
    }
}