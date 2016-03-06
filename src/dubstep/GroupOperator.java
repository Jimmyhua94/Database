package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.select.*;
import java.sql.SQLException;

public class GroupOperator implements Operator{
    
    Operator input;
    HashMap<String, Integer> schema;
    List<SelectItem> toProject;
    HashMap<String,Datum[]> groups;
    List<Column> condition;
    Expression having;
    
    //AGG
    public GroupOperator(Operator input,HashMap<String, Integer> schema, List<SelectItem> toProject){
        this.input = input;
        this.schema = schema;
        this.toProject = toProject;
        groups = new HashMap<String,Datum[]>();
    }
    //GROUP BY
    public GroupOperator(Operator input,HashMap<String, Integer> schema, List<SelectItem> toProject, List<Column> condition){
        this.input = input;
        this.schema = schema;
        this.toProject = toProject;
        this.condition = condition;
        groups = new HashMap<String,Datum[]>();
    }
    //HAVING
    public GroupOperator(Operator input,HashMap<String, Integer> schema, List<SelectItem> toProject, List<Column> condition, Expression having){
        this.input = input;
        this.schema = schema;
        this.toProject = toProject;
        this.condition = condition;
        this.having = having;
        groups = new HashMap<String,Datum[]>();
    }
    String hashFunction(String hash1,String hash2){
        return hash1+"hash"+hash2;
    }
    public Datum[] getNext(){
        Datum[] tuple = null;
        do{
            tuple = input.getNext();
            if (tuple != null){
                Evalator eval = new Evalator(schema, tuple);
                String hash = "";
                
                if(condition != null){
                    int i = 0;
                    hash = eval.eval(condition.get(i)).toString();
                    String val2 = "";
                    for(;i < condition.size()-1;i++){
                        val2 = eval.eval(condition.get(i+1)).toString();
                        hash = hashFunction(hash, val2);
                    }
                }
                
                for(SelectItem column: toProject){
                    Expression func = ((SelectExpressionItem)column).getExpression();
                    if (func instanceof Function){
                        Datum[] aggTuple = new Datum[tuple.length+1];
                        int j = 0;
                        for(Datum data: tuple){
                            aggTuple[j] = tuple[j];
                            j++;
                        }
                        tuple = aggTuple;
                        
                        schema.put(func.toString(),schema.size()-1);
                        tuple[tuple.length-1] = new Datum.Int("0");
                        
                        LongValue val = (LongValue)tuple[schema.get(((Function)func).toString())].getData();
                        if(groups.containsKey(hash)){
                            try{
                            LongValue prevVal = (LongValue)(groups.get(hash))[schema.get(((Function)func).toString())].getData();
                            LongValue colVal = (LongValue)eval.eval(((Function)func).getParameters().getExpressions().get(0));
                            if(((Function)func).getName().equals("COUNT"))
                                val.setValue(prevVal.getValue() + 1 );
                            if(((Function)func).getName().equals("AVG"))
                                val.setValue((prevVal.getValue()+colVal.getValue())/2);
                            if(((Function)func).getName().equals("MAX")){
                                if(prevVal.getValue() <= colVal.getValue())
                                    val.setValue(colVal.getValue());
                                else
                                    val.setValue(prevVal.getValue());
                            }
                            if(((Function)func).getName().equals("MIN")){
                                if(prevVal.getValue() >= colVal.getValue())
                                    val.setValue(colVal.getValue());
                                else
                                    val.setValue(prevVal.getValue());
                            }
                            if(((Function)func).getName().equals("SUM"))
                                val.setValue(prevVal.getValue()+colVal.getValue());
                            }
                            catch(SQLException e){
                                e.printStackTrace();
                            }
                        }
                        else{
                            try{
                                LongValue initVal = (LongValue)eval.eval(((Function)func).getParameters().getExpressions().get(0));
                                //LongValue initVal = (LongValue)(tuple[schema.get(((Function)func).getParameters().getExpressions().get(0).toString())].getData());
                                if(((Function)func).getName().equals("COUNT"))
                                    val.setValue(1);
                                if(((Function)func).getName().equals("AVG"))
                                    val.setValue(initVal.getValue());
                                if(((Function)func).getName().equals("MAX"))
                                    val.setValue(initVal.getValue());
                                if(((Function)func).getName().equals("MIN"))
                                    val.setValue(initVal.getValue());
                                if(((Function)func).getName().equals("SUM"))
                                    val.setValue(initVal.getValue());
                            }
                            catch(SQLException e){
                                e.printStackTrace();
                            }
                        }
                        tuple[tuple.length-1] = new Datum.Int(val.toString());
                    }
                }
                
                if(having != null){
                        try{
                            if(eval.eval(having).toBool()){
                                groups.put(hash,tuple);
                            }
                        }
                        catch(SQLException e){
                            e.printStackTrace();
                        }
                }
                else
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