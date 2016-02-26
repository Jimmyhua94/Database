package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.expression.*;
import java.sql.SQLException;

public class SelectionOperator implements Operator{
    
    Operator input;
    HashMap<String, Integer> schema;
    Expression condition;
    
    public SelectionOperator(Operator input, HashMap<String, Integer> schema, Expression condition){
        this.input = input;
        this.schema = schema;
        this.condition = condition;
    }
    
    public Datum[] getNext(){
        Datum[] tuple = null;
        do {
            tuple = input.getNext();
            if (tuple != null){
                Evalator eval = new Evalator(schema, tuple);
                try{
                    if(!eval.eval(condition).toBool()){
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