package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Parser {
    
    public static void main(String[] args){
        
        System.out.println("File compiled!");
        
        int i;
        File dataDir = null;
        ArrayList<File> sqlFiles = new ArrayList<File>();
        HashMap<String,CreateTable> tables = new HashMap<String,CreateTable>();
        
        for(i = 0;i < args.length;i++){
            //if(args[i].equals()){
            //}
            sqlFiles.add(new File(args[i]));
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
                        
                        List<ColumnDefinition> columnDef = ct.getColumnDefinitions();
                        //*This is just for debugging and testing
                        System.out.println("Columns: " + columnDef);
                        for(ColumnDefinition col : columnDef){
                            System.out.println("Column: "+col.getColumnName());
                        }
                        //*
                    }
                    else if(stmt instanceof Select){
                        SelectBody select = ((Select)stmt).getSelectBody();
                        
                        //DO SOMETHING
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
}