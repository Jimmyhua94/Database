package dubstep;

import java.io.*;
import java.util.*;
import java.sql.SQLException;


import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;

public class SelectionOperator implements Operator{
    
    Operator input;
    HashMap<String,HashMap<String,Integer>> schema;
    Expression condition;
    
    public SelectionOperator(Operator input, HashMap<String,HashMap<String,Integer>> schema, Expression condition){
        this.input = input;
        this.schema = schema;
        this.condition = condition;
    }
    
    public Datum[] getNext(){
        Datum[] tuple = null;
        do {
            tuple = input.getNext();
            if (tuple != null){
                String table = ((Column)((BinaryExpression)condition).getLeftExpression()).getTable().getName();
                Evalator eval = new Evalator(schema.get(table), tuple);
                try{
                    if(!eval.eval(condition).toBool()){
                        //System.err.println(eval.eval(condition));
                        tuple = null;
                    }
                }
                catch(SQLException e){
                    e.printStackTrace();
                }
            }
            else{
                return null;
            }
        } while (tuple == null);
        return tuple;
    }

    public void reset(){
        input.reset();
    }
}