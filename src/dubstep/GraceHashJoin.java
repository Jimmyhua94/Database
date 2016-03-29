package dubstep;

import java.util.*;
import java.sql.SQLException;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;

import dubstep.solution.data.TupleEval;
import dubstep.solution.plan.Operator;
import dubstep.solution.Schema;
import dubstep.solution.util.*;

public class GraceHashJoin extends Operator.Binary {
    
    Expression condition;
    boolean firstRead = true;
    boolean flip = false;
    HashMap<String,ArrayList<PrimitiveValue[]>> lhsMap;
    ArrayList<PrimitiveValue[]> joinTable;
    Iterator it;
    ArrayList<Column> lhsConditions;
    ArrayList<Column> rhsConditions;

  public GraceHashJoin(){ super(Operator.Type.JOIN); }
  public GraceHashJoin(Operator lhs, Operator rhs, Expression condition){ 
    super(Operator.Type.JOIN); 
    this.lhs = lhs;
    this.rhs = rhs;
    this.condition = condition;
    lhsMap = new HashMap<String,ArrayList<PrimitiveValue[]>>();
    joinTable = new ArrayList<PrimitiveValue[]>();
    
    lhsConditions = new ArrayList<Column>();
    rhsConditions = new ArrayList<Column>();
    
    Expression tempExpr = condition;
    Expression temp;
    do{
        if((temp = ((BinaryExpression)tempExpr).getRightExpression()) instanceof Column){
            temp = flipper(tempExpr);
            rhsConditions.add((Column)(((BinaryExpression)temp).getRightExpression()));
            lhsConditions.add((Column)(((BinaryExpression)temp).getLeftExpression()));
        }
        else if((temp = ((BinaryExpression)tempExpr).getRightExpression()) instanceof EqualsTo){
            temp = flipper(temp);
            rhsConditions.add((Column)(((BinaryExpression)temp).getRightExpression()));
            lhsConditions.add((Column)(((BinaryExpression)temp).getLeftExpression()));
        }
        tempExpr = ((BinaryExpression)tempExpr).getLeftExpression();
    }while(!(tempExpr instanceof Column));
  }
  
  private Expression flipper(Expression expr){
    /* If the condition is flipped, flip it */
    Set<Column> lhsCols = new HashSet<Column>(lhs.getSchema());
    Set<Column> rhsCols = new HashSet<Column>();
    
    rhsCols.add((Column)((BinaryExpression)expr).getLeftExpression());
    
    if(!(SetUtils.intersect(rhsCols, lhsCols).size() > 0)){
        Expression temp = ((BinaryExpression)expr).getLeftExpression();
        ((BinaryExpression)expr).setLeftExpression(((BinaryExpression)expr).getRightExpression());
        ((BinaryExpression)expr).setRightExpression(temp);
        flip = true;
    }
    return expr;
  }
  
  public String detailString(){
    return "GRACE HASH JOIN";
  }

  public List<Column> getSchema(){
    ArrayList<Column> vars = new ArrayList<Column>();
    vars.addAll(lhs.getSchema());
    vars.addAll(rhs.getSchema());
    return vars;
  }

  public void open() throws SQLException{
    lhs.open();
    rhs.open();
    
    /* Reset the iterator and initial joining */
    
    if(it != null){
        it = joinTable.iterator();
    }
    
    firstRead = true;
  }

  public void close() throws SQLException{
    lhs.close();
    rhs.close();
    it = null;
  }

  public PrimitiveValue[] getNext() throws SQLException{
    
    /* Do the Grace Hash Join */
    
    if(firstRead == true){
        PrimitiveValue[] lhsValue;
        TupleEval lhsEval = new TupleEval(lhs.getSchema());
        
        /* Read values in LHS and place them in buckets */
        
        while((lhsValue = lhs.getNext())!=null){
            lhsEval.setTuple(lhsValue);
            String key = "";
            for(Column col: lhsConditions){
                key = hashFunction(key,lhsEval.eval(col).toRawString());
            }
            if(!lhsMap.containsKey(key)){
                ArrayList<PrimitiveValue[]> bucket = new ArrayList<PrimitiveValue[]>();
                bucket.add(lhsValue);
                lhsMap.put(key,bucket);
            }
            else{
                lhsMap.get(key).add(lhsValue);
            }
        }
        
        /* Read values in RHS join them with all values in its matching LHS bucket */
    
        PrimitiveValue[] rhsValue;
        TupleEval rhsEval = new TupleEval(rhs.getSchema());
        
        while((rhsValue = rhs.getNext()) != null){
            rhsEval.setTuple(rhsValue);
            String key = "";
            for(Column col: rhsConditions){
                key = hashFunction(key,rhsEval.eval(col).toRawString());
            }
            
            if(lhsMap.containsKey(key)){
                ArrayList<PrimitiveValue[]> bucket = lhsMap.get(key);
                for(PrimitiveValue[] value: bucket){
                    PrimitiveValue[] tuple = new PrimitiveValue[value.length + rhsValue.length];
                    for(int i = 0; i < value.length; i++){
                        tuple[i] = value[i];
                    }
                    for(int i = 0; i < rhsValue.length; i++){
						tuple[i+value.length] = rhsValue[i];
                    }
                    joinTable.add(tuple);
                }
            }
        }
        
        firstRead=false;
    }
    
    /* Iterator so getNext() can return one tuple */

    if(it == null)
        it = joinTable.iterator();
    if(it.hasNext()){
        PrimitiveValue[] tup = (PrimitiveValue[])it.next();
        it.remove();
        return tup;
    }
    return null;
  }
  
    String hashFunction(String hash1,String hash2){
        return hash1+"hash"+hash2;
    }

}