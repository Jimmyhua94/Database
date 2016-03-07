package dubstep;

import java.io.*;
import java.util.*;
import java.sql.SQLException;
import java.util.Random;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.select.*;

public class GroupOperator implements Operator{
    
    Operator input;
    HashMap<String,HashMap<String,Integer>> schema;
    List<SelectItem> toProject;
    HashMap<String,Datum[]> groups;
    List<Column> condition;
    Expression having;
    Random rand;
    Iterator it;
    
    //AGG / PROJECT
    public GroupOperator(Operator input, HashMap<String,HashMap<String,Integer>> schema, List<SelectItem> toProject){
        this.input = input;
        this.schema = schema;
        this.toProject = toProject;
        groups = new HashMap<String,Datum[]>();
        rand = new Random();
    }
    //GROUP BY
    public GroupOperator(Operator input, HashMap<String,HashMap<String,Integer>> schema, List<SelectItem> toProject, List<Column> condition){
        this.input = input;
        this.schema = schema;
        this.toProject = toProject;
        this.condition = condition;
        groups = new HashMap<String,Datum[]>();
        rand = new Random();
    }
    //HAVING
    public GroupOperator(Operator input, HashMap<String,HashMap<String,Integer>> schema, List<SelectItem> toProject, List<Column> condition, Expression having){
        this.input = input;
        this.schema = schema;
        this.toProject = toProject;
        this.condition = condition;
        this.having = having;
        groups = new HashMap<String,Datum[]>();
        rand = new Random();
    }
    String hashFunction(String hash1,String hash2){
        return hash1+"hash"+hash2;
    }
    public Datum[] getNext(){
        String table = ((Column)((SelectExpressionItem)toProject.get(0)).getExpression()).getTable().getName();
        Datum[] tuple = null;
        do{
            tuple = input.getNext();
            if (tuple != null){
                Datum[] project = new Datum[toProject.size()];
                Evalator eval = new Evalator(schema.get(table), tuple);
                String hash = tuple[0].toString() + rand.nextInt((1000 - 0) + 1) + tuple[rand.nextInt((tuple.length - 0))].toString();
                
                if(condition != null){
                    int i = 0;
                    hash = eval.eval(condition.get(i)).toString();
                    String val2 = "";
                    for(;i < condition.size()-1;i++){
                        val2 = eval.eval(condition.get(i+1)).toString();
                        hash = hashFunction(hash, val2);
                    }
                }
                int i = 0;
                for(SelectItem column: toProject){
                    if (column instanceof AllColumns){
                        project = tuple;
                    }
                    else{
                        Expression func = ((SelectExpressionItem)column).getExpression();
                        if(func instanceof Column){
                            project[i] = tuple[schema.get(table).get(((Column)((SelectExpressionItem)toProject.get(i)).getExpression()).getColumnName())];
                        }
                        else if (func instanceof Function){
                            if(condition == null)
                                hash = "";
                            HashMap<String,Integer> cols = schema.get(table);
                            cols.put(func.toString(),schema.size());
                            //schema.put("groups",cols);
                            project[i] = new Datum.Int("0");
                            LongValue val = (LongValue)project[i].getData();
                            if(groups.containsKey(hash)){
                                try{
                                LongValue prevVal = (LongValue)(groups.get(hash))[i].getData();
                                if(((Function)func).getName().equals("COUNT"))
                                    val.setValue(prevVal.getValue() + 1 );
                                else{
                                    LongValue colVal = (LongValue)eval.eval(((Function)func).getParameters().getExpressions().get(0));
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
                                }
                                catch(SQLException e){
                                    e.printStackTrace();
                                }
                            }
                            else{
                                try{
                                    //LongValue initVal = (LongValue)(tuple[schema.get(((Function)func).getParameters().getExpressions().get(0).toString())].getData());
                                    if(((Function)func).getName().equals("COUNT"))
                                        val.setValue(1);
                                    else{
                                        LongValue initVal = (LongValue)eval.eval(((Function)func).getParameters().getExpressions().get(0));
                                        if(((Function)func).getName().equals("AVG"))
                                            val.setValue(initVal.getValue());
                                        if(((Function)func).getName().equals("MAX"))
                                            val.setValue(initVal.getValue());
                                        if(((Function)func).getName().equals("MIN"))
                                            val.setValue(initVal.getValue());
                                        if(((Function)func).getName().equals("SUM"))
                                            val.setValue(initVal.getValue());
                                    }
                                }
                                catch(SQLException e){
                                    e.printStackTrace();
                                }
                            }
                            //project[i] = new Datum.Int(val.toString());
                        }
                    }
                    i++;
                }
                
                if(having != null){
                        try{
                            if(eval.eval(having).toBool()){
                                groups.put(hash,project);
                            }
                        }
                        catch(SQLException e){
                            e.printStackTrace();
                        }
                }
                else
                    groups.put(hash,project);
            }
        }while(tuple != null);

        if(it == null)
            it = groups.entrySet().iterator();
        if (it.hasNext()){
            HashMap.Entry tup = (HashMap.Entry)it.next();
            it.remove();
            return (Datum[])tup.getValue();
        }
        return null;
    }
    
    public void reset(){
        //input.reset();
        it = groups.entrySet().iterator();
    }
}