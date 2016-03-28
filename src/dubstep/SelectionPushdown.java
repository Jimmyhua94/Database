package dubstep;

import java.util.*;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import dubstep.solution.plan.*;
import dubstep.solution.optimizer.*;
import dubstep.solution.util.*;

public class SelectionPushdown extends PlanRewrite {

	public SelectionPushdown(){ super(true); }

	public Expression makeAnd(Expression lhs, Expression rhs){
		if(lhs == null){ return rhs; }
		if(rhs == null){ return lhs; }
		return new AndExpression(lhs, rhs);
	}


	public Operator apply(Operator o)
	{
		if(o instanceof Selection){
			Selection select = (Selection)o;

			if(select.getChild() instanceof CrossProduct){
				CrossProduct cross = (CrossProduct)select.getChild();
				Operator lhs = cross.getLHS();
				Operator rhs = cross.getRHS();

				Set<Column> lhsCols = new HashSet<Column>(lhs.getSchema());
				Set<Column> rhsCols = new HashSet<Column>(rhs.getSchema());

				Expression lhsCondition = null;
				Expression rhsCondition = null;
				Expression topCondition = null;

				for(Expression clause : select.conjunctiveClauses()){
					System.out.println("CLAUSE: "+clause);
					Set<Column> clauseCols = FindExpressionSchema.find(clause);	
					System.out.println("   COLUMNS:"+clauseCols);
					// System.out.println("   Left:"+lhsCols);
					// System.out.println("   Right:"+rhsCols);
                    

					if(SetUtils.intersect(clauseCols, lhsCols).size() > 0){
						if(SetUtils.intersect(clauseCols, rhsCols).size() > 0){
							topCondition = makeAnd(topCondition, clause);
							System.out.println("  Keep it there!");
						} else {
							lhsCondition = makeAnd(lhsCondition, clause);
							System.out.println("  Push-down left");							
						}
					} else {
						rhsCondition = makeAnd(rhsCondition, clause);
						System.out.println("  Push-down right");							
					}

				}

				if(lhsCondition != null){
					lhs = new Selection(lhs, lhsCondition);
				}
				if(rhsCondition != null){
					rhs = new Selection(rhs, rhsCondition);
				}

				cross = new CrossProduct(lhs, rhs);
				if(topCondition != null){
					return new Selection(cross, topCondition);
				} else {
					return cross;
				}
			}
		}

		return o;
	}


}