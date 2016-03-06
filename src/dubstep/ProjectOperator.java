package dubstep;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.JSQLParserException;

public class ProjectOperator implements Operator{

 Operator input;
 HashMap<String, Integer> schema;
 List<SelectItem> toProject;
 
    ProjectOperator(Operator input,HashMap<String, Integer> schema, List<SelectItem> toProject){
        this.input = input;
        this.toProject = toProject;
        this.schema = schema;
    }

    public Datum[] getNext() {
        Datum[] tuple = null;
        tuple = input.getNext();
        if (toProject.get(0) instanceof AllColumns){
            return tuple;
        }
        else if (tuple != null){
            int i = 0;
            Datum[] project = new Datum[toProject.size()];
            for(SelectItem column: toProject){
                    project[i] = tuple[schema.get(column.toString())];
                i++;
            }
            return project;
        }
        else{
            return null;
        }
    }

    public void reset() {
    input.reset();
    }
}