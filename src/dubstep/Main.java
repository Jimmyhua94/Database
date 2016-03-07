package dubstep;

import java.io.*;
import java.util.*;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;

public class Main{

    public static void main(String[] args){

        File dataDir = null;
        ArrayList<File> sqlFiles = new ArrayList<File>();
        HashMap<String,CreateTable> tables = new HashMap<String,CreateTable>();

        for(int i = 0;i < args.length;i++){
            if(args[i].equals("--data")){
                dataDir = new File(args[++i]);
            }
            else {
                sqlFiles.add(new File(args[i]));
            }
        }

        for(File sql : sqlFiles){
            try{
                FileReader stream = new FileReader(sql);

                CCJSqlParser parser = new CCJSqlParser(stream);
                Statement stmt;
                FromScanner fromScan =  null;
                Operator oper = null;
                
                while((stmt = parser.Statement()) != null){
                    if(stmt instanceof CreateTable){
                        CreateTable ct = (CreateTable)stmt;
                        tables.put(ct.getTable().getName(),ct);
                    }
                    else if(stmt instanceof Select){
                        SelectBody select = ((Select)stmt).getSelectBody();
                        
                        if (select instanceof PlainSelect){
                            PlainSelect pSelect = ((PlainSelect)select);
                            
                            fromScan = new FromScanner(dataDir, tables);
                            
                            pSelect.getFromItem().accept(fromScan);
                            
                            if(pSelect.getJoins() != null)
                                pSelect.getJoins().get(0).getRightItem().accept(fromScan);
                            //System.err.println(pSelect.getJoins());
                            
                            oper = fromScan.source;

                            if(pSelect.getWhere() != null){
                                oper = new SelectionOperator(oper, fromScan.schemaCol, pSelect.getWhere());
                            }
                            if(pSelect.getHaving() != null)
                                oper = new GroupOperator(oper, fromScan.schemaCol,pSelect.getSelectItems(), pSelect.getGroupByColumnReferences(),pSelect.getHaving());
                            else if(pSelect.getGroupByColumnReferences() != null)
                                oper = new GroupOperator(oper, fromScan.schemaCol,pSelect.getSelectItems(), pSelect.getGroupByColumnReferences());
                            else
                                oper = new GroupOperator(oper, fromScan.schemaCol,pSelect.getSelectItems());
                            // oper = new ProjectOperator(oper, fromScan.schemaCol, pSelect.getSelectItems());
                            
                            dump(oper);
                        }
						else if(select instanceof Union){
							List<PlainSelect> unionItems = ((Union) select).getPlainSelects();
							//UNION FROM
							for(int temp = 0; temp<=unionItems.size()-1;temp++){
								if(unionItems.get(temp).getFromItem()!= null){
									fromScan = new FromScanner(dataDir, tables);
									unionItems.get(temp).getFromItem().accept(fromScan);
									oper = fromScan.source;
								}
								//UNION WHERE STATEMENT
								if(unionItems.get(temp).getWhere()!= null){
									oper = new SelectionOperator(oper, fromScan.schemaCol, unionItems.get(temp).getWhere());
								}
								if(unionItems.get(temp).getSelectItems()!= null){
									ArrayList<SelectExpressionItem> items = new ArrayList<SelectExpressionItem>();
									if(!(unionItems.get(temp).getSelectItems().get(0) instanceof AllColumns)){
										for (int n=0; n<=unionItems.get(temp).getSelectItems().size()-1; n++){
											items.add((SelectExpressionItem) unionItems.get(temp).getSelectItems().get(n));	
										}
                                        oper = new GroupOperator(oper, fromScan.schemaCol,unionItems.get(temp).getSelectItems());
										//oper = new ProjectOperator(oper, fromScan.schemaCol, unionItems.get(temp).getSelectItems());
									}
									dump(oper);
								}
								System.out.println("***********************************************************");
							}
						}
                    }
                    else{
                        System.out.println("PANIC!!!" + stmt);
                    }
                }
            }
            catch(IOException e){
                e.printStackTrace();
            }
            catch(ParseException e){
                e.printStackTrace();
            }
        }
    }

    public static void dump(Operator input){
        Datum[] row = input.getNext();
        while(row != null){
            for(int i = 0; i < row.length-1;i++){
                System.out.print(row[i] + "|");
            }
            System.out.println(row[row.length-1]);
            // for(Datum col : row){
                // System.out.print(col + "|");
            // }
            // System.out.println("");
            row = input.getNext();
        }
    }
}