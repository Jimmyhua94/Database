package dubstep;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import dubstep.solution.expr.*;
import java.util.*;

public class FindExpressionSchema extends ExpressionScan {
	public HashSet<Column> ret = new HashSet<Column>();

	public FindExpressionSchema(){ super(true); }

	public void visit(Column c){ ret.add(c); }

	public static HashSet<Column> find(Expression e){
		FindExpressionSchema scanner = new FindExpressionSchema();
		e.accept(scanner);
		return scanner.ret;
	}
}