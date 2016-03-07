package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;

public class FromScanner implements FromItemVisitor{
    File basePath;
    HashMap<String,CreateTable> tables;
    
    public HashMap<String,HashMap<String,Integer>> schemaCol = null;
    //public HashMap<String, Integer> schemaCol = null;
    public HashMap<Table,HashMap<Integer,ColumnDefinition>> schemas = null;
    public Operator source = null;
    
    public FromScanner(File basePath, HashMap<String,CreateTable> tables){
        this.basePath = basePath;
        this.tables = tables;
        schemas = new HashMap<Table,HashMap<Integer,ColumnDefinition>>();
        schemaCol = new HashMap<String,HashMap<String,Integer>>();
    }
    
    public void visit(SubJoin subJoin){
        
    }
    
    public void visit(SubSelect subSelect){
        
    }
    
    public void visit(Table tableName){
        CreateTable table = tables.get(tableName.getName());
        
        HashMap<Integer,ColumnDefinition> schema = new HashMap<Integer,ColumnDefinition>();

        schemas.put(tableName,schema);

        List<ColumnDefinition> columnDef = table.getColumnDefinitions();
        HashMap<String,Integer> sCol = new HashMap<String,Integer>();

        int i = 0;
        for(ColumnDefinition col : columnDef){
            sCol.put(col.getColumnName(),i);
            schema.put(i,col);
            i++;
        }
        if (tableName.getAlias() != null)
            schemaCol.put(tableName.getAlias(),sCol);
        else
            schemaCol.put(tableName.getName(),sCol);
        
        
        if (source == null)
            source = new ScanOperator(new File(basePath, tableName.getName() + ".dat"),schemas.get(tableName));
        else{
            Operator joinSource = new ScanOperator(new File(basePath, tableName.getName() + ".dat"),schemas.get(tableName));
            source = new JoinOperator(source, joinSource);
        }
    }
}