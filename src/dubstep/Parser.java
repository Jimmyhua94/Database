package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.eval.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import java.sql.SQLException;

public class Parser {
    
    public static void main(String[] args){

        System.out.println("File compiled!");

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
                            
                            dump(oper);
                            oper.reset();
                            GreaterThan cmp = new GreaterThan();
                            cmp.setLeftExpression(new Column(null, "A"));
                            
                            cmp.setRightExpression(new Column(null, "B"));
                            
                            Evalator test = new Evalator(fromScan.schemaCol,oper.getNext());
                            try{
                                System.out.println(test.eval(cmp));
                            }
                            catch(SQLException e){
                                e.printStackTrace();
                            }
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
            for(Datum col : row){
                System.out.print(col + "|");
            }
            System.out.println("");
            
            row = input.getNext();
        }
    }
}