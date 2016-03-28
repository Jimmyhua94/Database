package dubstep;

import java.util.*;

import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import dubstep.solution.plan.*;
import dubstep.solution.optimizer.*;
import dubstep.solution.util.*;

public class JoinOptimization extends PlanRewrite {
    
    public JoinOptimization(){ super(true); }
    
    public Operator apply(Operator o){
        if(o instanceof Selection){
            Selection select = (Selection)o;
            
            if(select.getChild() instanceof CrossProduct){
                CrossProduct cross = (CrossProduct)select.getChild();
				Operator lhs = cross.getLHS();
				Operator rhs = cross.getRHS();
                
				Set<Column> lhsCols = new HashSet<Column>(lhs.getSchema());
				Set<Column> rhsCols = new HashSet<Column>(rhs.getSchema());
                
                for(Expression clause : select.conjunctiveClauses()){
					System.out.println("CLAUSE: "+clause);
					Set<Column> clauseCols = FindExpressionSchema.find(clause);	
					System.out.println("   COLUMNS:"+clauseCols);
                    // System.out.println("   Left:"+lhsCols);
					// System.out.println("   Right:"+rhsCols);
                    
                    if(SetUtils.intersect(clauseCols, lhsCols).size() > 0){
						if(SetUtils.intersect(clauseCols, rhsCols).size() > 0){
                            Expression condition = select.getCondition();
                            GraceHashJoin join = new GraceHashJoin(lhs,rhs,condition);
                            System.out.println("  GraceHashJoin");
                            return join;
                        }
                    }
                }
            }
        }
        
        return o;
    }
}