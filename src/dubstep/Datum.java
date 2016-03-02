package dubstep;

import java.util.*;
import net.sf.jsqlparser.expression.*;
// import java.text.SimpleDateFormat;
import java.sql.Date;

public interface Datum<T>{
    T getData();
    String getType();
    String toString();

    public static class StringVal implements Datum<StringValue>{
        StringValue data;
        String type;
        public StringVal(String data){
            this.data = new StringValue(data);
            this.type = "string";
        }
        public StringValue getData(){
            return data;
        }
        public String getType(){
            return type;
        }
        public String toString(){
            return data.toRawString();
        }
    }
    public static class VarChar implements Datum<StringValue>{
        StringValue data;
        String type;
        public VarChar(String data){
            this.data = new StringValue(data);
            this.type = "varchar";
        }
        public StringValue getData(){
            return data;
        }
        public String getType(){
            return type;
        }
        public String toString(){
            return data.toRawString();
        }
    }
    public static class Char implements Datum<StringValue>{
        StringValue data;
        String type;
        public Char(String data){
            this.data = new StringValue(data);
            this.type = "char";
        }
        public StringValue getData(){
            return data;
        }
        public String getType(){
            return type;
        }
        public String toString(){
            return data.toRawString();
        }
    }
    public static class Int implements Datum<LongValue>{
        LongValue data; 
        String type;
        public Int(String data){
            this.data = new LongValue(data);
            this.type = "int";
        }
        public LongValue getData(){
            return data;
        }
        public String getType(){
            return type;
        }
        public String toString(){
            return data.toRawString();
        }
    }
    public static class Decimal implements Datum<DoubleValue>{
        DoubleValue data;
        String type;
        public Decimal(String data){
            this.data = new DoubleValue(data);
            this.type = "decimal";
        }
        public DoubleValue getData(){
            return data;
        }
        public String getType(){
            return type;
        }
        public String toString(){
            return data.toRawString();
        }
    }
    public static class DateVal implements Datum<DateValue>{
        DateValue data;
        String type;
        public DateVal(String data){
            this.data = new DateValue(data);
            this.type = "date";
        }
        public DateValue getData(){
            return data;
        }
        public String getType(){
            return type;
        }
        public String toString(){
            return data.toRawString();
        }
    }
    
}