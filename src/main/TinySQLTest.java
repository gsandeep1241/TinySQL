package main;

import storageManager.Schema;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;

public class TinySQLTest {

    public static void main(String[] args){
        String fileName = "resources/passing_test_queries.sql";
        ArrayList<String> list = new ArrayList<>();
        list.add("CREATE"); list.add("DROP"); list.add("INSERT"); list.add("SELECT"); list.add("DELETE");
        HashSet<String> set = new HashSet<>(list);

        TinySQL tinySQL = new TinySQL();
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));

            String sql;
            long start = System.currentTimeMillis();
            System.out.println("Start: " + start);
            while ((sql = br.readLine()) != null){
                if(set.contains(sql.split(" ")[0])){
                    solve(tinySQL, sql);
                }
            }
            long end = System.currentTimeMillis();
            System.out.println("End: " + end);
            long totalTimeSecs = (end-start)/1000;
            System.out.println("Total time: " + totalTimeSecs);
        }catch (Exception e){
            System.out.println("Error occurred.");
            e.printStackTrace();
        }
    }

    private static void solve(TinySQL tinySQL, String sql){
        System.out.println("Evaluating: " + sql);
        tinySQL.evaluate(sql, false);
        System.out.println();
    }
}
