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
    
    public GroupOperator(Operator input,HashMap<String, Integer> schema, List<SelectItem> toProject, List<Column> condition){
        this.input = input;
        this.schema = schema;
        this.toProject = toProject;
        this.condition = condition;
        groups = new HashMap<String,Datum[]>();
    }
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
                int i = 0;
                Evalator eval = new Evalator(schema, tuple);
                String hash = eval.eval(condition.get(i)).toString();
                String val2 = "";
                for(;i < condition.size();i++){
                    val2 = eval.eval(condition.get(i)).toString();
                    hash = hashFunction(hash, val2);
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
                        //Need to edit schema to add agg column
                        // if(((Function)func).getName() == "COUNT")
                            // tuple[schema.get((Function)func.toString())]++;
                        // if(((Function)func).getName() == "AVG")
                            // tuple[schema.get((Function)func.toString())] = (tuple[schema.get((Function)func.toString())]+eval.eval((Function)func.getParameters))/2;
                        // if(((Function)func).getName() == "MAX")
                            // if(tuple[schema.get((Function)func.toString())] <= eval.eval((Function)func.getParameters)))
                                // tuple[schema.get((Function)func.toString())] = eval.eval((Function)func.getParameters));
                        // if(((Function)func).getName() == "MIN")
                            // if(tuple[schema.get((Function)func.toString())] >= eval.eval((Function)func.getParameters)))
                                // tuple[schema.get((Function)func.toString())] = eval.eval((Function)func.getParameters));
                        // if(((Function)func).getName() == "SUM")
                            // tuple[schema.get((Function)func.toString())] += eval.eval((Function)func.getParameters));
                    }
                    i++;
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