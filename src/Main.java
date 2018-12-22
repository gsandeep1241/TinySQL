import main.TinySQL;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;

public class Main {

    private static void solve(TinySQL tinySQL, String sql){
        System.out.println("Evaluating: " + sql);
        tinySQL.evaluate(sql, false);
        System.out.println();
    }

    public static void main(String[] args){
        System.out.println("Welcome to TinySQL.");

        Scanner in = new Scanner(System.in);
        TinySQL tinySQLMain = new TinySQL();
        System.out.println("Enter 1 if you want to enter the query inline.");
        System.out.println("Enter 2 if you want to enter queries in a file.");
        System.out.println("Enter 3 to quit.");
        String input = in.nextLine();
        while(true){
            try {
                if(Integer.parseInt(input) == 1){
                    System.out.println("Enter the query.");
                    String sql = in.nextLine();
                    System.out.println();

                    ArrayList<String> list = new ArrayList<>();
                    list.add("CREATE"); list.add("DROP"); list.add("INSERT"); list.add("SELECT"); list.add("DELETE");
                    HashSet<String> set = new HashSet<>(list);
                    if(set.contains(sql.split(" ")[0])){
                        solve(tinySQLMain, sql);
                    }
                    continue;
                }else if(Integer.parseInt(input) == 2){
                    System.out.println("Enter file name. Ensure file is present in the current folder.");
                    System.out.println("All the previously entered queries will be disregarded.");
                    System.out.println("The program will exit after this.");
                    System.out.println();
                    String fileName = in.next();
                    System.out.println();
                    ArrayList<String> list = new ArrayList<>();
                    list.add("CREATE"); list.add("DROP"); list.add("INSERT"); list.add("SELECT"); list.add("DELETE");
                    HashSet<String> set = new HashSet<>(list);

                    TinySQL tinySQL = new TinySQL();
                    String sql = "";
                    try {
                        BufferedReader br = new BufferedReader(new FileReader(fileName));
                        System.out.println("Starting.. ");
                        long start = System.currentTimeMillis();
                        while ((sql = br.readLine()) != null) {
                            if (set.contains(sql.split(" ")[0])) {
                                solve(tinySQL, sql);
                            }
                        }
                        long end = System.currentTimeMillis();
                        long totalTimeSecs = (end - start) / 1000;
                        System.out.println("Total time: " + totalTimeSecs + " secs.");
                        System.out.println("Done.");
                    } catch (FileNotFoundException e){
                        System.out.println("File not found.");
                        System.out.println("Bye!");
                        break;
                    } catch (Exception e){
                        System.out.println("Error occurred for the query: " + sql);
                        System.out.println("Bye!");
                        break;
                    }
                    break;
                }else if(Integer.parseInt(input) == 3){
                    System.out.println("Bye!");
                    break;
                }else{
                    System.out.println("Please enter either 1, 2 or 3.");
                }
            }catch (Exception e){
                System.out.println("Please enter either 1, 2 or 3.");
            }

        }
    }
}
