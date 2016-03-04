package dubstep;

import java.io.*;
import java.util.*;

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
    }
    String hashFunction(String hash1,String hash2){
        return hash1+hash2;
    }
    Datum[] getNext(){
        Datum[] tuple = null;
        do{
            tuple = input.getNext();
            if (tuple != null){
                Evalator eval = new Evalator(schema, tuple);
                try{
                    String hash = eval.eval(condition.get(i)).getData().toString();
                    String val2 = "";
                    for(int i = 1;i < condition.size();i++){
                        val2 = eval.eval(condition.get(i)).getData().toString();
                        hash = hashFunction(hash, val2);
                    }
                    groups.put(hash,tuple);
                }
                catch(SQLException e){
                    e.printStackTrace();
                }
            }
        }while(tuple != null);
        Iterator it = groups.entrySet().iterator();
        if (it.hashNext()){
            return it.next().getValue();
        }
        return null;
    }
    void reset(){
        input.reset();
    }
}