package dubstep;

import java.util.*;

import net.sf.jsqlparser.eval.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;

public class Evalator extends Eval{
    
    HashMap<String, Integer> schema;
    Datum[] tuple;
    
    public Evalator(HashMap<String, Integer> schema, Datum[] tuple){
        this.schema = schema;
        this.tuple = tuple;
    }
    
    public PrimitiveValue eval(Column x){
        int colID = schema.get(x.getColumnName());
        return (PrimitiveValue)tuple[colID].getData();
    }
}