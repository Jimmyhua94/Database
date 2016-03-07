package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;


public class JoinOperator implements Operator {

	Operator source;
    Operator joinSource;
	HashMap<String, Integer> schemaCol;
    List<Datum[]> table;
    List<Datum[]> joinTable;
    Iterator it;
    

	JoinOperator(Operator source, Operator joinSource){
        this.source = source;
        this.joinSource = joinSource;
        this.schemaCol = schemaCol;
        joinTable = new ArrayList<Datum[]>();
    }
	
	public Datum[] getNext(){
        if(table == null){
            table = new ArrayList<Datum[]>();
            Datum[] tuple2;
            do{
                tuple2 = joinSource.getNext();
                table.add(tuple2);
            }while(tuple2 != null);
        }
        
        Datum[] tuple = null;
        
        do{
            tuple = source.getNext();
            if(tuple != null){
                for(int j = 0; j < table.size()-1;j++){
                    Datum[] join = new Datum[tuple.length+table.get(0).length];
                    int i = 0;
                    for(;i < tuple.length;i++){
                        join[i] = tuple[i];
                    }
                    for(int k = 0; k < table.get(j).length;k++){
                        join[i] = table.get(j)[k];
                        i++;
                    }
                    joinTable.add(join);
                }
            }
        }while(tuple != null);
        
        
        if(it == null)
            it = joinTable.iterator();
        if (it.hasNext()){
            Datum[] tup = (Datum[])it.next();
            it.remove();
            return tup;
        }
        return null;
	}
    
    public void reset(){
        //source.reset();
        //joinSource.reset();
        it = joinTable.iterator();
    }
	
}
