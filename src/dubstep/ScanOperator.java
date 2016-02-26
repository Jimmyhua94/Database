package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.statement.create.table.*;

public class ScanOperator implements Operator{
    
    File data;
    BufferedReader input;
    HashMap<Integer,ColumnDefinition> schema;
    
    public ScanOperator(File data,HashMap<Integer,ColumnDefinition> schema){
        this.data = data;
        this.schema = schema;
        reset();
    }
    
    public Datum[] getNext(){
        if(input != null){
            String line = null;
            try{
                line = input.readLine();
            }
            catch(IOException e){
                e.printStackTrace();
            }
            if(line != null){
                String[] cols = line.split("\\|");
                Datum[] ret = new Datum[cols.length];
                for(int i = 0; i < cols.length; i++){
                    String type;
                    ColumnDefinition col = schema.get(i);
                    type = col.getColDataType().getDataType();
                    switch(type) {
                        case "string":
                            ret[i] = new Datum.StringVal(cols[i]);
                            break;
                        case "varchar":
                            ret[i] = new Datum.VarChar(cols[i]);
                            break;
                        case "char":
                            ret[i] = new Datum.Char(cols[i]);
                            break;
                        case "int":
                            ret[i] = new Datum.Int(cols[i]);
                            break;
                        case "decimal":
                            ret[i] = new Datum.Decimal(cols[i]);
                            break;
                        case "date":
                            ret[i] = new Datum.DateVal(cols[i]);
                            break;
                        default:
                            System.out.println("Invalid data type.");
                            return null;
                    }
                }
                return ret;
            }
            return null;
        }
        return null;
    }
    
    public void reset(){
        try {
            input = new BufferedReader(new FileReader(data));
        }
        catch(IOException e){
            e.printStackTrace();
            input = null;
        }
    }
    
}