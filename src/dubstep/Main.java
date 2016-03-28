package dubstep;

import java.io.FileReader;
import java.io.File;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import java.sql.SQLException;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.delete.*;

import dubstep.solution.*;
import dubstep.solution.util.*;
import dubstep.solution.data.*;
import dubstep.solution.sql.*;
import dubstep.solution.plan.*;
import dubstep.solution.optimizer.*;


import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class Main {

  public enum ProjectStage 
    { Checkpoint0(false, false), 
      Checkpoint1(false, false), 
      Checkpoint2(true, false), 
      Checkpoint3(true, true);

      boolean useOptimizer;
      boolean useIndexes;

      ProjectStage(
        boolean useOptimizer, 
        boolean useIndexes
      ){ 
        this.useOptimizer = useOptimizer;
        this.useIndexes = useIndexes;
      }
    };

  protected static final ProjectStage projectStage = 
    ProjectStage.Checkpoint2;

  /**
   * Parse arguments and input files and dispatch requests to the appropriate 
   * system components
   *
   * Arguments:
   *  - '-d'       : Debug mode: Print out query information and related details.
   *  - '--test'   : Test run mode: Don't actually run the query (implies -d)
   *  - '--data d' : Specify a directory 'd' that contains data files.
   *  - '--o1'     : Checkpoint 2 and beyond: Don't optimize queries
   *  - '--swap s' : Checkpoint 3 and beyond: Specify a swap directory and activate out-of-core rewrites
   * @param argsArray   An array of command line arguments
   */
  public static void main(String[] argsArray)
    throws Exception
  {
    // System.setOut(new PrintStream(new FileOutputStream("File.out")));
      
    if(projectStage == ProjectStage.Checkpoint0) {
      System.out.println(
        "We, the members of our team, agree that we will not submit any "+
        "code that we have not written ourselves, share our code with "+
        "anyone outside of our group, or use code that we have not "+
        "written ourselves as a reference."
      );
      return;
    }

    ///// db stores all of the metadata related to a given run of the system/
    ///// This includes create table statements, index definitions, and file 
    ///// paths that are provided on the command line.
    Schema.Database db = new Schema.Database();
    
    ///// GetArgs is a utility class analogous to Unix's GetOpt.
    GetArgs args = new GetArgs(argsArray);
    
    ///// The current argument being parsed
    String arg;
    
    ///// Command-line options that affect control-flow and which components of
    ///// the system are invoked.
    boolean buildIndexes = projectStage.useIndexes;
    boolean dumpIndexes = false;
    boolean testOptimizer = false;
      
    if(projectStage.useOptimizer){
        Optimizer.setOptimizerActive(false);
    }
    
    // Start by extracting command-line arguments.  GetArgs automatically 
    // extracts arguments that are not parameters (i.e., that do not start
    // with a '-'
    while((arg = args.nextArg()) != null){
      if(arg.equals("-d")){
        db.debug = true;
      } else if(arg.equals("-o1")) {
        Optimizer.setOptimizerActive(false);
      } else if(arg.equals("--data")) {
        db.dataDir = new File(args.next());
      } else if(arg.equals("--swap")) {
        db.swapDir = new File(args.next());
      } else if(arg.equals("--index")) {
        db.indexDir = new File(args.next());
      } else if(arg.equals("--build")) {
        buildIndexes = true;
      } else if(arg.equals("--dumpIndex")) {
        dumpIndexes = true;
      } else if(arg.equals("--test")) {
        testOptimizer = true;
        db.debug = true;
      } else {
        System.out.println("Invalid argument: "+arg);
        System.exit(-1);
      }
    }

    // SqlToRA is responsible for translating from CCJSqlParser's output to 
    // our internal plan.Operator format, and extracting schema information from
    // create table statements.
    SqlToRA translator = new SqlToRA(db);
    
    // For each file name detected in the arguments, parse it in.
    for(String f : args.getFiles()){    
    
      CCJSqlParser parser = new CCJSqlParser(new FileReader(f));
      Statement s;
      
      // CCJSqlParser returns null once it hits EOF.
      while((s = parser.Statement()) != null){
        
        // Figure out what kind of statement we've just encountered

        if(s instanceof CreateTable){
          // SqlToRA extracts schema information from this create table 
          // statement and loads it into 'db'
          translator.loadTableSchema((CreateTable)s);


        } else if(s instanceof Select) {
          
          if(db.debug) { System.out.println("=== QUERY ===\n"+((Select)s).toString()); }
          
          // SqlToRA uses the visitor pattern to convert a SelectBody into the
          // corresponding Operator subclasses.
          Operator plan = translator.selectToPlan(((Select)s).getSelectBody());
          if(db.debug) { System.out.println("=== PLAN ===\n"+plan.toString()); }
          

          // Optimizer uses a set of heuristic rewrite rules to make the plan
          // more efficient.
          if(projectStage.useOptimizer){
            ArrayList<PlanRewrite> optimizations = new ArrayList<PlanRewrite>();
            // Add your optimizations here...
            optimizations.add(new SelectionPushdown());
            //optimizations.add(new JoinOptimization());

            plan = Optimizer.optimize(plan, db, optimizations);
          }
          if(db.debug) { System.out.println("=== OPTIMIZED PLAN ===\n"+plan.toString()); }
        
          // Stop here if we received '--test' on the command line.
          if(!testOptimizer){
            if(db.debug) { System.out.println("=== RESULTS ==="); }
            
            // compile() invokes eval.PlanCompiler to convert a logical plan 
            // (a tree of annotated relational algebra operators) into the 
            // corresponding physical plan (a composable Iterator-like 
            // interface called eval.Operator).
            
            // dump() uses the Operator's read() method to iterate over the 
            // output of the compiled query.
            dump(plan);
          }
        } else {
        
          // Utility method that produces an "Unsupported Feature" exception
          translator.feature("Unhandled Statement Type");
        }
        
        // The visitor pattern doesn't play nicely with exceptions.  If an 
        // exception occurs during translation, checkError() will re-raise it 
        // here.
        translator.checkError();

      } // END if(statement instanceof Select)

    } // END while((s = parser.Statement()) != null)

  }
  
  /**
   * Read in the output of a physical operator and dump it to stdout.
   * @param data  The physical operator to dump to stdout.
   * @throws SQLException if operator evaluation encounters a runtime error.
   */
  public static void dump(Operator data)
    throws SQLException
  {
    PrimitiveValue[] tuple;
    data.open();
    while((tuple = data.getNext()) != null){
      String sep = "";
      for(PrimitiveValue d : tuple){
        System.out.print(sep+d);
        sep = "|";
      }
      System.out.print("\n");
    }
    data.close();
  }
}
