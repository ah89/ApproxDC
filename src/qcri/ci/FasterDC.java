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

import javafx.util.Pair;
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

public class FasterDC {

    protected long startingTime;

    public Table originalTable;

    //The space of all DCs found
    protected ArrayList<DenialConstraint> totalDCs = new ArrayList<DenialConstraint>();

    //The space of all predicates
    public ArrayList<Predicate> presMap = new ArrayList<>();

    public Long evidenceNum;
    public Map<Long, Set<Predicate>> evidenceMap = new HashMap<>();
    public Map<Predicate, Set<Long>> evidenceForPred = new HashMap<>();
    public ArrayList<Set<Predicate>> evidenceSet = new ArrayList<>();
    //public Map<Set<Predicate>,Long> evidence = new HashMap<Set<Predicate>,Long>();

    private ArrayList<Long> uncovered = new ArrayList<>();
    private Map<Predicate, Set<Long>> crit = new HashMap<>();

    private ArrayList<Predicate> candidates = new ArrayList<>();

    public DBProfiler dbPro;

    public FasterDC(String inputDBPath, int numRows)
    {
        startingTime = System.currentTimeMillis();
        originalTable = new Table(inputDBPath, numRows);


        System.out.println("before find all predicate");
        findAllPredicates();
        System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
                "Number of all predicates: " + presMap.size());


        initAllTupleWiseInfoParallel();
        System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
                "Done init evidences: " + evidenceNum);
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
        Integer index_predicate = -1;

        for(int col = 1; col <= numCols; col++)
        {

            Predicate pre = new Predicate(originalTable,"EQ",1,2,col,col);
            presMap.add(pre);
            candidates.add(pre);

            pre = new Predicate(originalTable,"IQ",1,2,col,col);
            presMap.add(pre);
            candidates.add(pre);


            int type = originalTable.getColumnMapping().positionToType(col);
            if(type == 0 || type == 1)
            {
                pre = new Predicate(originalTable,"GT",1,2,col,col);
                presMap.add(pre);
                candidates.add(pre);

                pre = new Predicate(originalTable,"LTE",1,2,col,col);
                presMap.add(pre);
                candidates.add(pre);

                pre = new Predicate(originalTable,"LT",1,2,col,col);
                presMap.add(pre);
                candidates.add(pre);

                pre = new Predicate(originalTable,"GTE",1,2,col,col);
                presMap.add(pre);
                candidates.add(pre);
            }
        }

        dbPro = new DBProfiler(originalTable);
/*
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



        }*/
        System.out.println("Number of variable predicates: " + presMap.size());
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

        final ArrayList<Set<Set<Predicate>>> tempPairInfo = new ArrayList<>();


        System.out.println("We are going to use " + numThreads + " threads");
        final int numRows = originalTable.getNumRows();
        int chunk = numRows / numThreads;

        for(int k = 0 ; k < numThreads; k++)
        {
            final int kTemp = k;
            final int startk = k * chunk;
            final int finalk = (k==numThreads-1)? numRows:(k+1)*chunk;
            //tempInfo.add(new HashMap<Set<Predicate>,Long>());

            Set<Set<Predicate>> tempPairInfok = new HashSet<>();
            tempPairInfo.add(tempPairInfok);
            Thread thread = new Thread(new Runnable()
            {
                public void run() {
                    Set<Set<Predicate>> tempPairInfok = tempPairInfo.get(kTemp);

                    int numCols = originalTable.getNumCols();
                    for(int t1 = startk ; t1 < finalk; t1++)
                        for(int t2 = 0 ; t2 < numRows; t2++)
                        {

                            Tuple tuple1 = originalTable.getTuple(t1);
                            Tuple tuple2 = originalTable.getTuple(t2);

                            if(tuple1.tid == tuple2.tid)
                                continue;

                            Set<Predicate> cur = new HashSet<>();

                            //The naive way

                            int p = 0;

                            if(Config.qua == 1)
                            {
                                for(int col = 1; col <= numCols; col++)
                                {
                                    Predicate pre = presMap.get(p);
                                    int type = originalTable.getColumnMapping().positionToType(col);

                                    if(type == 2)
                                    {
                                        if(dbPro.equalOrNot(col-1, tuple1.getCell(col-1).getValue(), tuple2.getCell(col-1).getValue()))
                                        {

                                            cur.add(pre);

                                        }
                                        else
                                        {
                                            cur.add(presMap.get(p+1));

                                        }
                                        p = p + 2;
                                    }
                                    else if(type == 0 || type == 1)
                                    {
                                        // if >
                                        if(presMap.get(p+2).check(tuple1,tuple2))
                                        {
                                            cur.add(presMap.get(p+1));
                                            cur.add(presMap.get(p+2));
                                            cur.add(presMap.get(p+5));

                                        }
                                        else // <=
                                        {
                                            // =
                                            if(presMap.get(p).check(tuple1, tuple2))
                                            {
                                                cur.add(pre);
                                                cur.add(presMap.get(p+3));
                                                cur.add(presMap.get(p+5));



                                            }
                                            else
                                            {
                                                cur.add(presMap.get(p+1));
                                                cur.add(presMap.get(p+3));
                                                cur.add(presMap.get(p+4));



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


/*                            for(p = p ; p <= presNum; p+=2)
                            {

                                Predicate pre = presMap.get(p);
                                if(pre.check(tuple1,tuple2))
                                {
                                    cur.add(p);
                                }
                                else
                                    cur.add(p+1);



                            }*/

                            tempPairInfok.add(cur);
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
        Long evidenceSetIndex = 0L;
        for(int k = 0 ; k < numThreads; k++)
        {
            Set<Set<Predicate>> tempPairInfok = tempPairInfo.get(k);
            for(Set<Predicate> cur: tempPairInfok)
            {
                if (evidenceSet.contains(cur))
                    continue;


                evidenceSetIndex = evidenceSetIndex + 1;
                evidenceMap.put(evidenceSetIndex, cur);
                uncovered.add(evidenceSetIndex);
                evidenceSet.add(cur);

                for (Predicate i : cur) {
                    if (evidenceForPred.get(i) == null)
                        evidenceForPred.put(i, new HashSet<>());

                    evidenceForPred.get(i).add(evidenceSetIndex);
                }
            }
        }

        evidenceNum = evidenceSetIndex;

//        Set<Integer> tempind;
//        for (Set<Predicate> k : evidence.keySet()) {
//            tempind = predicates_to_indexes(k);
//            evidence_ind.add(tempind);
//        }
//
//        for (Set<Integer> intset : evidence_ind)
//            System.out.println(intset.toString() + ',');
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
        if(presMap.contains(pre))
        {
            int index = presMap.indexOf(pre);
            if(index %2 == 0)
                return presMap.get(index+1);
            else
                return presMap.get(index - 1);
        }
        else
        {
            System.out.println("ERROR: Getting the reverse of a predicate.");
            return null;
        }
    }
    private Map<Predicate,Set<Predicate>> impliedMap = new HashMap<>();
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
            for(Predicate temp: presMap)
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

    private Map<Predicate,Set<Predicate>> sameAttMap = new HashMap<>();
    public Set<Predicate> getPredicatesSameAtt(Predicate pre)
    {
        if(pre == null)
            return new HashSet<Predicate>();

        if(!sameAttMap.containsKey(pre))
        {
            Set<Predicate> result = new HashSet<Predicate>();
            for(Predicate temp: presMap)
            {
                if (Arrays.equals(temp.getCols(), pre.getCols())) {
                    result.add(temp);
                }
            }
            sameAttMap.put(pre, result);
            return result;
        }
        else
            return sameAttMap.get(pre);
    }



    public void dc2File()
    {
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
        //Do not discover DCs containing done
        System.out.println("Discovering...");
        //Set<Predicate> done = new HashSet<Predicate>();
        /*for(Predicate curPre: allPres)
        {
            if(curPre.getName().equals("GTE") || curPre.getName().equals("LTE"))
            {
                if(!curPre.isSecondCons())
                    done.add(curPre);
            }

        }*/

        Set<Set<Predicate>> allMVC = new HashSet<>();
        if(evidenceNum > 0L)
            generateMHS(allMVC);
        else
            allMVC.add(new HashSet<>());

        for(Set<Predicate> mvc: allMVC)
        {
            //Reverse the predicate
            ArrayList<Predicate> reversedMVC = new ArrayList<>();
            for(Predicate pre: mvc)
            {
                Predicate rPre = getReversePredicate(pre);
                reversedMVC.add(rPre);
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

    private Set<Predicate> IntToPreSet(Set<Integer> preSet) {
        Set<Predicate> result = new HashSet<>();
        for (Integer i : preSet) {
            result.add(presMap.get(i));
        }

        return result;
    }

    private void updateCritUncov( Predicate currPred, Set<Predicate> currSet ) {
        if ( crit.get( currPred ) == null ) { // initialize critical set for the new predicate
            crit.put(currPred, new HashSet<>());
        }

        for ( Long evidenceSet : evidenceForPred.get(currPred) ) {
            for ( Predicate p : currSet ) {
                Set<Long> critPred = crit.get( p );

                if ( critPred != null && critPred.contains( evidenceSet )) {
                    critPred.remove( evidenceSet );
                    crit.put(p, critPred);
                }
            }

            if ( uncovered.contains( evidenceSet ) ) {
                uncovered.remove( evidenceSet );
                crit.get( currPred ).add(evidenceSet);
            }
        }
    }

    private boolean checkNewHS(Predicate oldPred, Predicate newPred1, Predicate newPred2) {
        Set<Long> critical = crit.get(oldPred);

        Set<Long> evidence1 = evidenceForPred.get(newPred1);
        Set<Long> evidence2 = evidenceForPred.get(newPred2);

        if (evidence1 == null && evidence2 == null) {
            return false;
        }

        boolean result = true;

        if (evidence1 != null) {
            for (Long set : critical) {
                if (!evidence1.contains(set)) {
                    result = false;
                    break;
                }
            }

            if (result) {
                return true;
            }
        }

        if (evidence2 != null) {
            for (Long set : critical) {
                if (!evidence2.contains(set)) {
                    result = false;
                    break;
                }
            }
        }

        return result;
    }

    private Set<Predicate> findMinSAT() {
        int min = Integer.MAX_VALUE;
        Set<Predicate> minSAT = new HashSet<>();

        for (Long set : uncovered) {
            int num = 0;
            Set<Predicate> ev = evidenceMap.get(set);

            for (Predicate p : ev) {
                if (candidates.contains(p)) {
                    num++;
                }
            }

            if (num < min) {
                min = num;
                minSAT = ev;
            }
        }

        return minSAT;
    }

    private void generateMHS( Set<Set<Predicate>> hittingSets, Set<Predicate> curr ) {
        /*if(Config.dfsLevel != -1)
        {
            if(curr.size() + 1 > Config.dfsLevel)
                return;
        }*/

        if ( uncovered.size() == 0 ) { // all sets are covered - minimal hitting set
            for (Predicate pred : curr) {
                int p = presMap.indexOf(pred);
                if (originalTable.getColumnMapping().positionToType(pred.getCols()[0]) == 2) {
                    continue;
                }

                if (pred.getName().equalsIgnoreCase("IQ") && checkNewHS(pred, presMap.get(p+1), presMap.get(p+3))) {
                    return;
                }
                else if (pred.getName().equalsIgnoreCase("LTE") && checkNewHS(pred, presMap.get(p+1), presMap.get(p-3))) {
                    return;
                }
                else if (pred.getName().equalsIgnoreCase("GTE") && checkNewHS(pred, presMap.get(p-3), presMap.get(p-5))) {
                    return;
                }
            }


            System.out.println((System.currentTimeMillis() - startingTime) / 1000 + ":" +
                "New Hitting Set: " + curr.toString());

            hittingSets.add( new HashSet<>(curr) );
            return;
        }

        ArrayList<Predicate> oldCandidates = new ArrayList<>(candidates);

        Set<Predicate> uncovSet = findMinSAT();

        //Set<Predicate> uncovSet = evidenceMap.get(uncovered.iterator().next());

        ArrayList<Predicate> currCand = new ArrayList<>(); // currCand - all candidates that belong to uncovSet
        for ( Predicate pred : uncovSet ) {
             if ( candidates.contains( pred ) ) {
                 currCand.add( pred );
                 candidates.remove( pred ); // candidates = candidates \ currCand
             }
        }

        ArrayList<Long> uncoveredOld = new ArrayList<>(uncovered);

        HashMap<Predicate, Set<Long>> critOld = new HashMap<>();
        for (Predicate key: crit.keySet()) {
            critOld.put(key, new HashSet<>(crit.get(key)));
        }

        for ( Predicate pred : currCand ) { // try to add to the current set each element of CurrCand
            updateCritUncov( pred, curr );

            boolean minimalityCheck = true; // check minimality condition
            for ( Predicate p : curr ) {
                if ( crit.get( p ).size() == 0 ) {
                    minimalityCheck = false;
                    break;
                }
            }

            if ( minimalityCheck ) {
                Set<Predicate> candPredToRemove = getPredicatesSameAtt( pred );
                Set<Predicate> predToRemove = new HashSet<>();
                for (Predicate p : candPredToRemove) {
                    if (candidates.contains(p)) {
                        predToRemove.add(p);
                    }
                }
                candidates.removeAll(predToRemove);
                curr.add( pred ); // recursive call with the new set containting the current predicate
                generateMHS( hittingSets, curr );
                candidates.addAll(predToRemove);
                candidates.add(pred);
            }

            curr.remove( pred );

            uncovered = new ArrayList<>(uncoveredOld); // restore uncovered and critical sets
            crit = new HashMap<>();
            for (Predicate key: critOld.keySet()) {
                crit.put(key, new HashSet<>(critOld.get(key)));
            }
        }

        candidates = new ArrayList<>(oldCandidates);
    }

    public void generateMHS (Set<Set<Predicate>> hittingSets) {
        System.out.println("Generating MHS...");
        generateMHS( hittingSets, new HashSet<>() );
    }
}
