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
        
        File dataDir = null;
        ArrayList<File> sqlFiles = new ArrayList<File>();
        HashMap<String,CreateTable> tables = new HashMap<String,CreateTable>();
        HashMap<String,HashMap<String,Integer>> schemas = new HashMap<String,HashMap<String,Integer>>();
        
        for(int i = 0;i < args.length;i++){
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

                        HashMap<String,Integer> schema = new HashMap<String,Integer>();
                        schemas.put(ct.getTable().getName(),schema);

                        List<ColumnDefinition> columnDef = ct.getColumnDefinitions();

                        int i = 0;
                        for(ColumnDefinition col : columnDef){
                            schema.put(col.getColumnName(),i++);
                        }
                    }
                    else if(stmt instanceof Select){
                        SelectBody select = ((Select)stmt).getSelectBody();
                        PlainSelect s = ((PlainSelect)select);
                        
                        if(s.getFromItem()!= null){
                            try{
                                String parsedLine = null;
                                String delim = "[|]+";
                                //Will replace with non static file path.
                                FileReader fileReader = new FileReader("sql/"+s.getFromItem().toString().toUpperCase()+".dat");
                                BufferedReader data = new BufferedReader(fileReader);
                                while ((parsedLine = data.readLine()) != null){
                                    String[] tokens = parsedLine.split(delim);
                                    //*Used for debugging
                                    System.out.println(parsedLine);
                                    for(String column : tokens){
                                        System.out.println(column);
                                    }
                                    //*
                                }
                            }
                            catch(IOException e){
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
}