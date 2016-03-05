package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import java.sql.SQLException;

public class GroupOperator implements Operator{
    
    Operator input;
    HashMap<String, Integer> schema;
    HashMap<String,Datum[]> groups;
    List<Column> condition;
    
    public GroupOperator(Operator input,HashMap<String, Integer> schema, List<Column> condition){
        this.input = input;
        this.schema = schema;
        this.condition = condition;
        groups = new HashMap<String,Datum[]>();
    }
    String hashFunction(String hash1,String hash2){
        return hash1+hash2;
    }
    public Datum[] getNext(){
        Datum[] tuple = null;
        do{
            tuple = input.getNext();
            if (tuple != null){
                int i = 0;
                Evalator eval = new Evalator(schema, tuple);
                String hash = eval.eval(condition.get(i)).toString();
                String val2 = "";
                for(;i < condition.size();i++){
                    val2 = eval.eval(condition.get(i)).toString();
                    hash = hashFunction(hash, val2);
                }
                groups.put(hash,tuple);
            }
        }while(tuple != null);
        Iterator it = groups.entrySet().iterator();
        if (it.hasNext()){
            HashMap.Entry tup = (HashMap.Entry)it.next();
            it.remove();
            return (Datum[])tup.getValue();
        }
        return null;
    }
    
    public void reset(){
        input.reset();
    }
}