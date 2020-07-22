package qcri.ci;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import qcri.ci.generaldatastructure.constraints.DBProfiler;
import qcri.ci.generaldatastructure.constraints.DenialConstraint;
import qcri.ci.generaldatastructure.constraints.Predicate;
import qcri.ci.generaldatastructure.db.ColumnMapping;
import qcri.ci.generaldatastructure.db.MyConnection;
import qcri.ci.generaldatastructure.db.Table;
import qcri.ci.generaldatastructure.db.Tuple;
import qcri.ci.instancedriven.SingleTupleConstantDCMining;
import qcri.ci.utils.Config;
import qcri.ci.utils.OperatorMapping;

public class FASTDC {

    protected long startingTime;

    public Table originalTable;

    //The space of all DCs found
    protected ArrayList<DenialConstraint> totalDCs = new ArrayList<DenialConstraint>();

    //The space of all predicates
    public ArrayList<Predicate> allPres = new ArrayList<Predicate>();

    public Map<Set<Predicate>,Long> evidence = new HashMap<Set<Predicate>,Long>();

    private ArrayList<Set<Integer>> evidence_ind = new ArrayList<>();

    public DBProfiler dbPro;

    public FASTDC(String inputDBPath, int numRows)
    {
        startingTime = System.currentTimeMillis();
        originalTable = new Table(inputDBPath, numRows);


        System.out.println("before find all predicate");
        findAllPredicates();
        System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
                "Number of all predicates: " + allPres.size());


        initAllTupleWiseInfoParallel();
        System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
                "Done init evidences: " + evidence.size());
    }


    /**
     * This is the function where you can add all predicates that you are interested in
     * The set of operators you are interested in
     * Allowing comparison within a tuple?
     * Allowing comparison cross columns?
     */
    private void findAllPredicates()
    {

        int numCols = originalTable.getNumCols();
        int index_predicate = 0;
        for(int col = 1; col <= numCols; col++)
        {

            Predicate pre = new Predicate(originalTable,"EQ",1,2,col,col);
            pre.setIndex(index_predicate);
            index_predicate = index_predicate + 1;
            allPres.add(pre);
            pre = new Predicate(originalTable,"IQ",1,2,col,col);
            pre.setIndex(index_predicate);
            index_predicate = index_predicate + 1;
            allPres.add(pre);


            int type = originalTable.getColumnMapping().positionToType(col);
            if(type == 0 || type == 1)
            {
                pre = new Predicate(originalTable,"GT",1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);
                pre = new Predicate(originalTable,"LTE",1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);

                pre = new Predicate(originalTable,"LT",1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);

                pre = new Predicate(originalTable,"GTE",1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);
            }

        }

        dbPro = new DBProfiler(originalTable);

        if(Config.enableCrossColumn)
        {
            Set<String> eqCompa = dbPro.getEquComCols();
            Set<String> orderCompa = dbPro.getOrderComCols();

            int[] row1s = null;
            int[] row2s = null;
            if(Config.ps == 1)
            {
                row1s = new int[]{1,1};
                row2s = new int[]{1,2};
            }
            else if(Config.ps == 2)
            {
                row1s = new int[]{1,2,1,2};
                row2s = new int[]{1,2,2,1};
            }



            //3. EQ, IQ for column pairs
            for(String cols: eqCompa)
            {
                System.out.println("Joinale columns: " + cols);
                String[] colsTemp = cols.split(",");
                int col1 = originalTable.getColumnMapping().NametoPosition(colsTemp[0]);
                int col2 = originalTable.getColumnMapping().NametoPosition(colsTemp[1]);
                for(int i = 0; i <  row1s.length; i++)
                {
                    int row1 = row1s[i];
                    int row2 = row2s[i];
                    Predicate pre = new Predicate(originalTable,"EQ",row1,row2,col1,col2);
                    allPres.add(pre);
                    //singleTupleVarPre.add(pre);
                    pre = new Predicate(originalTable,"IQ",row1,row2,col1,col2);
                    allPres.add(pre);
                    //singleTupleVarPre.add(pre);
                }
            }
            // 4. GL, LTE for each column pair with numerical values
            for(String cols: orderCompa)
            {
                System.out.println("Order Comparable columns: " + cols);
                String[] colsTemp = cols.split(",");
                int col1 = originalTable.getColumnMapping().NametoPosition(colsTemp[0]);
                int col2 = originalTable.getColumnMapping().NametoPosition(colsTemp[1]);
                for(int i = 0; i <  row1s.length; i++)
                {
                    int row1 = row1s[i];
                    int row2 = row2s[i];
                    Predicate pre = new Predicate(originalTable,"GT",row1,row2,col1,col2);
                    allPres.add(pre);
                    pre = new Predicate(originalTable,"LTE",row1,row2,col1,col2);
                    allPres.add(pre);
                    pre = new Predicate(originalTable,"LT",row1,row2,col1,col2);
                    allPres.add(pre);
                    pre = new Predicate(originalTable,"GTE",row1,row2,col1,col2);
                    allPres.add(pre);
                }
            }



        }
        System.out.println("Number of variable predicates: " + allPres.size());
        //ArrayList<Predicate> consPres = dbPro.findkfrequentconstantpredicate();
        //System.out.println("NUmber of constant predicates: " + consPres.size());
        //allPres.addAll(consPres);

    }

    /**
     * Initialize all the tuple pair wise information for each set of constant predicates
     */
    private void initAllTupleWiseInfoParallel()
    {

        int numThreads =  Runtime.getRuntime().availableProcessors();

        ArrayList<Thread> threads = new ArrayList<Thread>();

        final ArrayList<Map<Set<Predicate>, Long>> tempPairInfo
                = new ArrayList<Map<Set<Predicate>, Long>>();


        System.out.println("We are going to use " + numThreads + " threads");
        final int numRows = originalTable.getNumRows();
        int chunk = numRows / numThreads;

        for(int k = 0 ; k < numThreads; k++)
        {
            final int kTemp = k;
            final int startk = k * chunk;
            final int finalk = (k==numThreads-1)? numRows:(k+1)*chunk;
            //tempInfo.add(new HashMap<Set<Predicate>,Long>());

            Map<Set<Predicate>, Long> tempPairInfok = new HashMap<Set<Predicate>, Long>();
            tempPairInfo.add(tempPairInfok);
            Thread thread = new Thread(new Runnable()
            {
                public void run() {
                    Map<Set<Predicate>, Long> tempPairInfok = tempPairInfo.get(kTemp);

                    int numCols = originalTable.getNumCols();
                    for(int t1 = startk ; t1 < finalk; t1++)
                        for(int t2 = 0 ; t2 < numRows; t2++)
                        {

                            Tuple tuple1 = originalTable.getTuple(t1);
                            Tuple tuple2 = originalTable.getTuple(t2);

                            if(tuple1.tid == tuple2.tid)
                                continue;

                            Set<Predicate> cur = new HashSet<Predicate>();

                            //The naive way

                            int p = 0;

                            if(Config.qua == 1)
                            {
                                for(int col = 1; col <= numCols; col++)
                                {

                                    Predicate pre = allPres.get(p);

                                    int type = originalTable.getColumnMapping().positionToType(col);

                                    if(type == 2)
                                    {

                                        int rel = 0; // 0- EQ, -1 -> IQ, 1- > , 2- <
                                        if(dbPro.equalOrNot(col-1, tuple1.getCell(col-1).getValue(), tuple2.getCell(col-1).getValue()))
                                        {

                                            rel = 0;
                                            cur.add(pre);

                                        }
                                        else
                                        {
                                            cur.add(allPres.get(p+1));


                                            rel = -1;
                                        }
                                        p = p + 2;
                                    }
                                    else if(type == 0 || type == 1)
                                    {
                                        // if >
                                        if(allPres.get(p+2).check(tuple1,tuple2))
                                        {
                                            cur.add(allPres.get(p+1));
                                            cur.add(allPres.get(p+2));
                                            cur.add(allPres.get(p+5));

                                        }
                                        else // <=
                                        {
                                            // =
                                            if(allPres.get(p).check(tuple1, tuple2))
                                            {
                                                cur.add(allPres.get(p));
                                                cur.add(allPres.get(p+3));
                                                cur.add(allPres.get(p+5));



                                            }
                                            else
                                            {
                                                cur.add(allPres.get(p+1));
                                                cur.add(allPres.get(p+3));
                                                cur.add(allPres.get(p+4));



                                            }

                                        }

                                        p = p + 6;
                                    }
                                    else
                                    {
                                        System.out.println("Type not supported");
                                    }

                                }
                            }


                            for(p = p ; p < allPres.size(); p+=2)
                            {

                                Predicate pre = allPres.get(p);
                                if(pre.check(tuple1,tuple2))
                                {
                                    cur.add(pre);
                                }
                                else
                                    cur.add(allPres.get(p+1));



                            }

                            if (tempPairInfok.containsKey(cur))
                            {
                                tempPairInfok.put(cur, tempPairInfok.get(cur)+1);


                            }
                            else
                            {
                                tempPairInfok.put(cur, (long) 1);

                            }



                        }
                }

            });
            thread.start();
            threads.add(thread);
        }

        for(int k = 0 ; k < numThreads; k++)
        {
            try {
                threads.get(k).join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        for(int k = 0 ; k < numThreads; k++)
        {
            Map<Set<Predicate>, Long> tempPairInfok = tempPairInfo.get(k);
            for(Set<Predicate> cur: tempPairInfok.keySet())
            {
                if (evidence.containsKey(cur))
                {
                    evidence.put(cur, evidence.get(cur) + tempPairInfok.get(cur));

                }
                else
                {
                    System.out.println(cur.toString());
                    evidence.put(cur, tempPairInfok.get(cur));
                }

            }


        }

/*        Set<Integer> tempind;
        for (Set<Predicate> k : evidence.keySet()) {
            tempind = predicates_to_indexes(k);
            evidence_ind.add(tempind);
        }

        for (Set<Integer> intset : evidence_ind)
            System.out.println(intset.toString() + ',');*/
    }


    private Set<Integer> predicates_to_indexes(Set<Predicate> preset) {
        Set<Integer> indexes = new HashSet<>();
        for (Predicate p : preset) {
            indexes.add(p.getIndex());
        }
        return indexes;
    }




    /**
     * Get the reverse predicate of all predicates
     * @param pre
     * @return
     */
    public Predicate getReversePredicate(Predicate pre)
    {
        if(allPres.contains(pre))
        {
            int index = allPres.indexOf(pre);
            if(index %2 == 0)
                return allPres.get(index+1);
            else
                return allPres.get(index - 1);
        }
        else
        {
            System.out.println("ERROR: Getting the reverse of a predicate.");
            return null;
        }
    }

    private Map<Predicate,Set<Predicate>> impliedMap = new HashMap<Predicate,Set<Predicate>>();
    /**
     * Get the set of predicates implied by the passed-in predicate, does not include the pre itself
     * @param pre
     * @return
     */
    public Set<Predicate> getImpliedPredicates(Predicate pre)
    {
        if(pre == null)
            return new HashSet<Predicate>();

        if(!impliedMap.containsKey(pre))
        {
            Set<Predicate> result = new HashSet<Predicate>();
            for(Predicate temp: allPres)
            {
                if(temp == pre)
                    continue;
                if(pre.implied(temp))
                    result.add(temp);
            }
            impliedMap.put(pre, result);
            return result;



        }
        else
            return impliedMap.get(pre);
    }



    public void dc2File()
    {
        System.out.println("Printing to file...");
        String dcPath = new String((new StringBuilder(originalTable.getCurrentFolder())).append("dcsFASTDC"));

        try {
            PrintWriter out = new PrintWriter(new FileWriter(dcPath));
            out.println("The set of Denial Constraints:");
            for(DenialConstraint dc: totalDCs)
            {
                out.println(dc.interestingness + ":" + dc.toString());
            }
            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public Set<Set<Predicate>> discover()
    {
        System.out.println("Discovering...");

        //Do not discover DCs containing done
        Set<Predicate> done = new HashSet<>();
        for(Predicate curPre: allPres)
        {
            if(curPre.getName().equals("GTE") || curPre.getName().equals("LTE"))
            {
                if(!curPre.isSecondCons())
                    done.add(curPre);
            }

        }

        Set<Set<Predicate>> allMVC = new HashSet<Set<Predicate>>();
        if(evidence.size()!=0)
            getAllMVCDFS(allMVC,evidence,done,totalDCs);
        else
            allMVC.add(new HashSet<Predicate>());

        for(Set<Predicate> mvc: allMVC)
        {
            //Reverse the predicate
            ArrayList<Predicate> reversedMVC = new ArrayList<Predicate>();
            for(Predicate pre: mvc)
            {
                Predicate aaa = getReversePredicate(pre);
                reversedMVC.add(aaa);
            }
            DenialConstraint dc = new DenialConstraint(2, reversedMVC);
            totalDCs.add(dc);
        }


        System.out.println((System.currentTimeMillis() - startingTime)/1000 + ":" +
                "Total number of DCs: " + totalDCs.size());
        System.out.println((System.currentTimeMillis() - startingTime)/1000 + ":" +
                "*************Done Constraints Discovery******************");

        return allMVC;
    }
    private void getAllMVCDFS(Set<Set<Predicate>> allMVCs, Map<Set<Predicate>,Long> toBeCovered,Set<Predicate> myDone,
                              ArrayList<DenialConstraint> dcs)
    {
        //Get the ordering of predicate, based on their coverage
        Set<Predicate> curMVC = new HashSet<Predicate>();
        ArrayList<Predicate> candidates = new ArrayList<Predicate>();
        for(Predicate temp: allPres)
        {
            for(Set<Predicate> key: toBeCovered.keySet())
                if(key.contains(temp))
                {
                    Predicate reverse = getReversePredicate(temp);
                    if(myDone.contains(reverse))
                    {
                        //This predicate cannot be used to generate cover
                        break;
                    }
                    else
                    {
                        //OK, this is a valid candidate
                        candidates.add(temp);
                        break;
                    }

                }
        }
        ArrayList<Predicate> ordering = getOrdering2(toBeCovered,candidates,curMVC);
        System.out.println("Generating hitting sets...");
        getAllMVCDFSRecursive(allMVCs,toBeCovered,ordering,curMVC,myDone,dcs,null,toBeCovered);
        System.out.println("Done generating hitting sets...");
    }
    private void getAllMVCDFSRecursive(Set<Set<Predicate>> allMVCs,Map<Set<Predicate>,Long> toBeCovered,
                                       ArrayList<Predicate> ordering,Set<Predicate> curMVC,Set<Predicate> myDone, ArrayList<DenialConstraint> dcs,Predicate lastMVC,
                                       Map<Set<Predicate>,Long> OriginalToBeCovered)
    {


        //If the size of the branch gets too long, return

        /*if(Config.dfsLevel != -1)
        {
            if(curMVC.size() + 1 > Config.dfsLevel)
                return;
        }*/



        //subset pruning
        boolean tag = false;
        for(Set<Predicate> tempMVC: allMVCs)
        {
            if(curMVC.containsAll(tempMVC))
            {
                //countPruningInDFS1++;
                tag = true;
                break;
            }

        }
        if(tag)
            return;

        long maxVios = 0;
        if(Config.noiseTolerance != 0)
            maxVios = getMaxVios(curMVC);

        long remainingVios = 0;
        for(Long value: toBeCovered.values())
            remainingVios += value;
        if(ordering.size() == 0 && remainingVios > maxVios)
            return;
        if(remainingVios <= maxVios)
        {
            if(!minimalityTest(curMVC,OriginalToBeCovered))
                return;
            Set<Predicate> newMVC = new HashSet<Predicate>(curMVC);
            allMVCs.add(newMVC);
            System.out.println((System.currentTimeMillis() - startingTime) / 1000 + ":" +
                    "New Hitting Set: " + newMVC.toString());
            return;
        }

        ArrayList<Predicate> remaining = new ArrayList<Predicate>(ordering);
        for(int i = 0 ; i < ordering.size(); i++)
        {

            Predicate curPre = ordering.get(i);
            remaining.remove(curPre);


            Map<Set<Predicate>,Long> nextToBeCovered = new HashMap<Set<Predicate>,Long>();
            for(Set<Predicate> temp2: toBeCovered.keySet())
            {
                if(!temp2.contains(curPre))
                {
                    nextToBeCovered.put(temp2, toBeCovered.get(temp2));
                }
            }

            curMVC.add(curPre);
            ArrayList<Predicate> nextOrdering = getOrdering2(nextToBeCovered,remaining, curMVC);
            getAllMVCDFSRecursive(allMVCs,nextToBeCovered,nextOrdering,curMVC,myDone,dcs,curPre,OriginalToBeCovered);
            curMVC.remove(curPre);
        }
    }

    private ArrayList<Predicate> getOrdering2(Map<Set<Predicate>,Long> toBeCovered,ArrayList<Predicate> candidate, Set<Predicate> curMVC)
    {
        ArrayList<Predicate> result = new ArrayList<Predicate>();
        for(Predicate pre: candidate)
        {
            //Get rid of trivial DC at this point
            boolean tagTrivial = false;
            Predicate reverse = getReversePredicate(pre);
            Set<Predicate> revImplied = getImpliedPredicates(reverse);
            for(Predicate temp: curMVC)
            {
                if(temp == pre)
                    continue;
                if(reverse == temp || revImplied.contains(temp))
                {
                    tagTrivial = true;
                }
            }
            if(tagTrivial)
                continue;


            long count = 0;
            for(Set<Predicate> temp: toBeCovered.keySet())
                if(temp.contains(pre))
                {
                    if(Config.noiseTolerance == 0)
                        count++;
                    else
                        count += toBeCovered.get(temp);
                }

            if(count == 0)
            {
                //assert(false);
                continue;
            }


            result.add(pre);
        }
        return result;


    }


    /**
     * To test whether there is a (n-1) subset of curMVC, that covers toBeCovered
     * @param curMVC
     * @param OriginalToBeCovered
     * @return
     */
    private boolean minimalityTest(Set<Predicate> curMVC, Map<Set<Predicate>,Long> OriginalToBeCovered)
    {
        if(Config.noiseTolerance != 0)
        {
            Set<Predicate> tempMVC = new HashSet<Predicate>(curMVC);
            for(Predicate leftOut: curMVC)
            {
                tempMVC.remove(leftOut);
                //Test whether temMVC is a cover or not
                long numVios = 0;
                for(Set<Predicate> key: OriginalToBeCovered.keySet())
                {
                    boolean thisKeyCovered = false;
                    for(Predicate pre: tempMVC)
                        if(key.contains(pre))
                        {
                            thisKeyCovered = true;
                            break;
                        }
                    if(!thisKeyCovered)
                    {
                        numVios += OriginalToBeCovered.get(key);
                    }
                }
                if(numVios < getMaxVios(curMVC))
                {
                    return false;
                }

                tempMVC.add(leftOut);
            }
            return true;
        }
        else
        {
            Set<Predicate> tempMVC = new HashSet<Predicate>(curMVC);
            for(Predicate leftOut: curMVC)
            {
                tempMVC.remove(leftOut);
                //Test whether temMVC is a cover or not
                boolean isCover = true;
                for(Set<Predicate> key: OriginalToBeCovered.keySet())
                {
                    boolean thisKeyCovered = false;
                    for(Predicate pre: tempMVC)
                        if(key.contains(pre))
                        {
                            thisKeyCovered = true;
                            break;
                        }
                    if(!thisKeyCovered)
                    {
                        isCover = false;
                        break;
                    }
                }
                if(isCover)
                {
                    return false;
                }

                tempMVC.add(leftOut);
            }
            return true;
        }


    }
    public long getMaxVios(Set<Predicate> pres)
    {
        boolean twoTuple = false;
        for(Predicate temp: pres)
        {
            int[] rows = temp.getRows();
            if(rows[0] != rows[1])
            {
                twoTuple = true;
                break;
            }
        }
        if(twoTuple)
        {
            return  ((long)(originalTable.getNumRows()  * Config.noiseTolerance) )
                    *  originalTable.getNumRows();
        }
        else
            return  ((long)(originalTable.getNumRows() * originalTable.getNumRows()
                    * Config.noiseTolerance))
                    * originalTable.getNumRows() ;
    }


}
