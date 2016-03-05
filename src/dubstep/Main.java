package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main{

    public static void main(String[] args){

        File dataDir = null;
        ArrayList<File> sqlFiles = new ArrayList<File>();
        HashMap<String,CreateTable> tables = new HashMap<String,CreateTable>();

        for(int i = 0;i < args.length;i++){
            if(args[i].equals("--data")){
                dataDir = new File(args[++i]);
            }
            else {
                sqlFiles.add(new File(args[i]));
            }
        }

        for(File sql : sqlFiles){
            try{
                FileReader stream = new FileReader(sql);

                CCJSqlParser parser = new CCJSqlParser(stream);
                Statement stmt;
                
                while((stmt = parser.Statement()) != null){
                    if(stmt instanceof CreateTable){
                        CreateTable ct = (CreateTable)stmt;
                        tables.put(ct.getTable().getName(),ct);
                    }
                    else if(stmt instanceof Select){
                        SelectBody select = ((Select)stmt).getSelectBody();
                        
                        if (select instanceof PlainSelect){
                            PlainSelect pSelect = ((PlainSelect)select);
                            
                            FromScanner fromScan = new FromScanner(dataDir, tables);
                            
                            pSelect.getFromItem().accept(fromScan);
                            
                            Operator oper = fromScan.source;
                            
                            if(pSelect.getWhere() != null){
                                oper = new SelectionOperator(oper, fromScan.schemaCol, pSelect.getWhere());
                            }
                            if(pSelect.getGroupByColumnReferences() != null){
                                if(pSelect.getHaving() != null)
                                    oper = new GroupOperator(oper, fromScan.schemaCol,pSelect.getSelectItems(), pSelect.getGroupByColumnReferences(),pSelect.getHaving());
                                else
                                    oper = new GroupOperator(oper, fromScan.schemaCol,pSelect.getSelectItems(), pSelect.getGroupByColumnReferences());
                            }
                            oper = new ProjectOperator(oper, fromScan.schemaCol, pSelect.getSelectItems());
                            
                            dump(oper);
                        }
                    }
                    else{
                        System.out.println("PANIC!!!" + stmt);
                    }
                }
            }
            catch(IOException e){
                e.printStackTrace();
            }
            catch(ParseException e){
                e.printStackTrace();
            }
        }
    }

    public static void dump(Operator input){
        Datum[] row = input.getNext();
        while(row != null){
            for(int i = 0; i < row.length-1;i++){
                System.out.print(row[i] + "|");
            }
            System.out.println(row[row.length-1]);
            // for(Datum col : row){
                // System.out.print(col + "|");
            // }
            // System.out.println("");
            row = input.getNext();
        }
    }
}