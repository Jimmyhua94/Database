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
    
    public HashMap<String, Integer> schemaCol = null;
    public HashMap<String,HashMap<Integer,ColumnDefinition>> schemas = null;
    public Operator source = null;
    
    public FromScanner(File basePath, HashMap<String,CreateTable> tables){
        this.basePath = basePath;
        this.tables = tables;
        schemas = new HashMap<String,HashMap<Integer,ColumnDefinition>>();
        schemaCol = new HashMap<String, Integer>();
    }
    
    public void visit(SubJoin subJoin){
        
    }
    
    public void visit(SubSelect subSelect){
        
    }
    
    public void visit(Table tableName){
        CreateTable table = tables.get(tableName.getName());
        HashMap<Integer,ColumnDefinition> schema = new HashMap<Integer,ColumnDefinition>();
        schemas.put(tableName.getName(),schema);

        List<ColumnDefinition> columnDef = table.getColumnDefinitions();

        int i = 0;
        for(ColumnDefinition col : columnDef){
            schemaCol.put(col.getColumnName(),i);
            schema.put(i,col);
            i++;
        }
        if (source == null)
            source = new ScanOperator(new File(basePath, tableName.getName() + ".dat"),schemas.get(tableName.getName()));
        else{
            Operator joinSource = new ScanOperator(new File(basePath, tableName.getName() + ".dat"),schemas.get(tableName.getName()));
            source = new JoinOperator(source, joinSource);
        }
    }
}