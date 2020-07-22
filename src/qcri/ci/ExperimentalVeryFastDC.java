package qcri.ci;

import com.sun.org.apache.bcel.internal.generic.NEW;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javafx.util.Pair;
import qcri.ci.generaldatastructure.constraints.DBProfiler;
import qcri.ci.generaldatastructure.constraints.NewDenialConstraint;
import qcri.ci.generaldatastructure.constraints.NewPredicate;
import qcri.ci.generaldatastructure.constraints.NewPredicate;
import qcri.ci.generaldatastructure.constraints.Predicate;
import qcri.ci.generaldatastructure.db.NewTable;
import qcri.ci.generaldatastructure.db.NewTuple;
import qcri.ci.generaldatastructure.db.Table;
import qcri.ci.generaldatastructure.db.Tuple;
import qcri.ci.utils.BooleanPair;
import qcri.ci.utils.Config;
import qcri.ci.utils.IntegerPair;

public class ExperimentalVeryFastDC {

    protected long startingTime;

    public NewTable originalTable;

    public int numOfChecks = 0;

    int evidenceSetNum = 0;

    //The space of all DCs found
    protected ArrayList<NewDenialConstraint> totalDCs = new ArrayList<>();

    //The space of all predicates
    public ArrayList<NewPredicate> allPres = new ArrayList<>();
    public ArrayList<NewPredicate> allPresSorted = new ArrayList<>();

    public Map<Set<NewPredicate>,Long> evidence = new HashMap<Set<NewPredicate>,Long>();

    public DBProfiler dbPro;

    public int numOfTuples;
    public int numOfCols;

    protected ArrayList<NewPredicate> candidates = new ArrayList<>();
    protected Map<NewPredicate, Set<IntegerPair>> crit = new HashMap<>();

    public Map<IntegerPair, Set<NewPredicate>> evidenceSet = new HashMap<>();
    public Map<IntegerPair, Set<NewPredicate>> reverseEvidenceSet = new HashMap<>();

    public int totalNumDCs = 0;

    public ExperimentalVeryFastDC(String inputDBPath, int numRows)
    {
        startingTime = System.currentTimeMillis();
        originalTable = new NewTable(inputDBPath, numRows);
        numOfTuples = numRows;


        System.out.println("before find all predicate");
        findAllNewPredicates();
        System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
                "Number of all predicates: " + allPres.size());

        //generateDCs();
    }

    private void updateCritNew( NewPredicate currPred, Set<NewPredicate> currSet, IntegerPair newCovered) {
        Set<IntegerPair> newSet = new HashSet<>();
        newSet.add(newCovered);
        crit.put(currPred, newSet);

        for (NewPredicate p : currSet) {
            Set<IntegerPair> critPred = crit.get(p);
            Set<IntegerPair> toRemove = new HashSet<>();

            for (IntegerPair pair : critPred) {
                int[] pairValue = pair.getPair();
                NewTuple tuple1 = originalTable.getTuple(pairValue[0]);
                NewTuple tuple2 = originalTable.getTuple(pairValue[1]);

                if (currPred.check(tuple1, tuple2)) {
                    toRemove.add(pair);
                }
            }

            critPred.removeAll(toRemove);
        }
    }

    private void updateCritOld( Set<NewPredicate> currSet, int startLocal, int startGlobal, int endLocal, int endGlobal) {
        for (int i = startGlobal; i < endGlobal; i++) {
           for (int j = startLocal; j<= i; j++) {
               NewTuple tuple1 = originalTable.getTuple(j);
               NewTuple tuple2 = originalTable.getTuple(i);

               List<NewPredicate> cover1 = new ArrayList<>();
               List<NewPredicate> cover2 = new ArrayList<>();
               for (NewPredicate pred : currSet) {
                   if (pred.check(tuple1, tuple2)) {
                       cover1.add(pred);
                   }
                   if (pred.check(tuple2, tuple1)) {
                       cover2.add(pred);
                   }
               }

               if (cover1.size() == 1)
                   crit.get(cover1.get(0)).add(new IntegerPair(j,i));
               if (cover1.size() == 1)
                   crit.get(cover2.get(0)).add(new IntegerPair(i,j));
           }

           startLocal = 0;
        }

        for (int i = startLocal; i <= endLocal; i++) {
            NewTuple tuple1 = originalTable.getTuple(i);
            NewTuple tuple2 = originalTable.getTuple(endGlobal);

            List<NewPredicate> cover1 = new ArrayList<>();
            List<NewPredicate> cover2 = new ArrayList<>();
            for (NewPredicate pred : currSet) {
                if (pred.check(tuple1, tuple2)) {
                    cover1.add(pred);
                }
                if (pred.check(tuple2, tuple1)) {
                    cover2.add(pred);
                }
            }

            if (cover1.size() == 1)
                crit.get(cover1.get(0)).add(new IntegerPair(i, endGlobal));
            if (cover1.size() == 1)
                crit.get(cover2.get(0)).add(new IntegerPair(endGlobal, i));
        }
    }

    /*private boolean checkNewHS(NewPredicate oldPred, NewPredicate newPred1, NewPredicate newPred2) {
        Set<Long> critical = crit.get(oldPred);

        List<Long> evidence1 = evidenceForPred.get(newPred1);
        List<Long> evidence2 = evidenceForPred.get(newPred2);

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

        result = true;

        if (evidence2 != null) {
            for (Long set : critical) {
                if (!evidence2.contains(set)) {
                    result = false;
                    break;
                }
            }
        }

        return result;
    }*/

    private Map<NewPredicate,Set<NewPredicate>> sameAttMap = new HashMap<>();
    public Set<NewPredicate> getNewPredicatesSameAtt(NewPredicate pre)
    {
        if(pre == null)
            return new HashSet<NewPredicate>();

        if(!sameAttMap.containsKey(pre))
        {
            Set<NewPredicate> result = new HashSet<NewPredicate>();
            for(NewPredicate temp: allPres)
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

    /*private boolean isCovered(Set<NewPredicate> mhs) {
        int mhsSize = mhs.size();

        if (mhsSize == 0) {
            return evidenceNum == 0L;
        }

        Long uncoveredSize = 0L;
        for (Long l : uncovered) {
            uncoveredSize = uncoveredSize + evidenceSet.get(l);
        }

        if (mhsSize == 1) {
            return ((double)(evidenceNum -uncoveredSize) / (double)evidenceNum) >= 0.999999;
        }

        Map<Integer, List<Pair<List<Long>, Integer>>> union = new HashMap();
        union.put(1, new ArrayList<>());

        int index = 0;
        for (NewPredicate p : mhs) {
            union.get(1).add(new Pair<>(evidenceForPred.get(p), index));
            index++;
        }

        for (int k=2; k < mhsSize; k++) {

            union.put(k, new ArrayList<>());
            int currSize = union.get(k-1).size();

            for (int i = 0; i < currSize; i++) {
                List<Long> first = union.get(k-1).get(i).getKey();
                int from = union.get(k-1).get(i).getValue();

                for (int j = from+1; j < currSize; j++) {

                    Pair<List<Long>, Integer> result = new Pair<>(new ArrayList<>(), j);
                    List<Long> second = union.get(1).get(j).getKey();

                    int ai = 0, bi = 0, ci = 0;
                    while (ai < first.size() && bi < second.size()) {
                        if (first.get(ai) < second.get(bi)) {
                            ai++;
                        }
                        else if (first.get(ai) > second.get(bi)) {
                            bi++;
                        }
                        else {
                            result.getKey().add(first.get(ai));
                            ci++;
                            ai++; bi++;
                        }
                    }

                    union.get(k).add(result);
                }
            }
        }

        List<List<Long>> finalResults = new ArrayList<>();
        for (Pair<List<Long>, Integer> pair : union.get(mhsSize-1)) {
            finalResults.add(pair.getKey());
        }

        Set<Long> result = new HashSet<>();
        for (List<Long> list : finalResults) {
            result.addAll(list);
        }

        Long numOfCovers = 0L;
        for (Long l : result) {
            numOfCovers = numOfCovers + evidenceSet.get(l);
        }

        return ((double)(evidenceNum - uncoveredSize) / (double)numOfCovers) >= 0.999999;
    }*/

    private Set<NewPredicate> linearGetClosure(Set<NewPredicate> premise)
    {
        //Init of the result
        Set<NewPredicate> result = new HashSet<NewPredicate>(premise);
        for(NewPredicate temp: premise)
        {
            result.addAll(this.getImpliedNewPredicates(temp));
        }
        //allImplied(result);


        //A list of candidate DCs, n-1 predicates are already in result
        List<NewDenialConstraint> canDCs = new ArrayList<NewDenialConstraint>();

        //From predicate, to a set of DCs, that contain the key predicate
        Map<NewPredicate,Set<NewDenialConstraint>> p2dcs = new HashMap<NewPredicate,Set<NewDenialConstraint>>();
        //From DC, to a set of predicates, that are not yet included in the closure
        Map<NewDenialConstraint,Set<NewPredicate>> dc2ps = new HashMap<NewDenialConstraint,Set<NewPredicate>>();


        //for(NewPredicate pre: allVarPre)
        //p2dcs.put(pre, new HashSet<NewDenialConstraint>());
        //Init two maps
        for(NewDenialConstraint dc: totalDCs)
        {
            for(NewPredicate pre: dc.getPredicates())
            {
                if(p2dcs.containsKey(pre))
                {
                    Set<NewDenialConstraint> value = p2dcs.get(pre);
                    value.add(dc);
                    p2dcs.put(pre, value);
                }
                else
                {
                    Set<NewDenialConstraint> value = new HashSet<NewDenialConstraint>();
                    value.add(dc);
                    p2dcs.put(pre, value);
                }


            }
            dc2ps.put(dc, new HashSet<NewPredicate>());
        }
        for(NewPredicate pre: p2dcs.keySet())
        {

            if(result.contains(pre))
                continue;


            for(NewDenialConstraint tempDC: p2dcs.get(pre))
            {
                Set<NewPredicate> value = dc2ps.get(tempDC);
                value.add(pre);
                dc2ps.put(tempDC, value);
            }
        }
        //Init the candidate list
        for(NewDenialConstraint dc: totalDCs)
        {
            if(dc2ps.get(dc).size() == 1)
                canDCs.add(dc);
            if(dc2ps.get(dc).size() == 0)
            {
                result.add(null);
                return result;
            }
        }

        //Do the main loop of adding to the list
        while(!canDCs.isEmpty())
        {
            NewDenialConstraint canDC = canDCs.remove(0);

            Set<NewPredicate> tailSet = dc2ps.get(canDC);

            //Why can be 0?
            if(tailSet.size() == 0)
                continue;

            NewPredicate tail = tailSet.iterator().next();

            NewPredicate reverseTail = this.getReverseNewPredicate(tail);


            Set<NewPredicate> toBeProcessed = new HashSet<NewPredicate>();
            toBeProcessed.add(reverseTail);
            toBeProcessed.addAll(this.getImpliedNewPredicates(reverseTail));
            toBeProcessed.removeAll(result);


            for(NewPredicate reverseTailImp: toBeProcessed)
            {
                if(p2dcs.containsKey(reverseTailImp))
                {
                    Set<NewDenialConstraint> covered = p2dcs.get(reverseTailImp);
                    Set<NewDenialConstraint> toBeRemoved = new HashSet<NewDenialConstraint>();
                    for(NewDenialConstraint tempDC: covered)
                    {
                        Set<NewPredicate> value = dc2ps.get(tempDC);
                        value.remove(reverseTailImp);
                        if(value.size() == 1)
                        {
                            canDCs.add(tempDC);
                            toBeRemoved.add(tempDC);
                        }
                        else if(value.size() == 0)
                        {
                            result.add(null);
                            return result;
                        }

                    }
                    covered.removeAll(toBeRemoved);
                }
            }

            result.addAll(toBeProcessed);

            //allImplied(result);

        }



        return result;
    }

    public boolean linearImplication(NewDenialConstraint conse)
    {
        if(conse == null)
            return false;

        Set<NewPredicate> premise = new HashSet<NewPredicate>(conse.getPredicates());
        Set<NewPredicate> closure = linearGetClosure(premise);
        if(closure.contains(null))
            return true;
        else
            return false;
    }

    public void buildSATForPair(Set<NewPredicate> result, Set<NewPredicate> reverseResult, NewTuple tuple1, NewTuple tuple2) {
        evidenceSetNum++;

        int p = 0;
        for(int col = 1; col <= numOfCols; col++)
        {
            NewPredicate pre = allPres.get(p);

            int type = originalTable.getColumnMapping().positionToType(col);

            if(type == 2)
            {
                if(dbPro.equalOrNot2(col-1, tuple1.getCell(col-1).getStringValue(), tuple2.getCell(col-1).getStringValue()))
                {
                    result.add(pre);
                    reverseResult.add(allPres.get(p+1));
                }
                else
                {
                    result.add(allPres.get(p+1));
                    reverseResult.add(pre);
                }
                p = p + 2;
            }
            else if(type == 0 || type == 1)
            {
                // >
                if(allPres.get(p+2).check(tuple1,tuple2))
                {
                    result.add(allPres.get(p+2));
                    result.add(allPres.get(p+1));
                    result.add(allPres.get(p+5));

                    reverseResult.add(pre);
                    reverseResult.add(allPres.get(p+3));
                    reverseResult.add(allPres.get(p+4));
                }
                else // <=
                {
                    // =
                    if(allPres.get(p).check(tuple1, tuple2))
                    {
                        result.add(allPres.get(p));
                        result.add(allPres.get(p+3));
                        result.add(allPres.get(p+5));

                        reverseResult.add(allPres.get(p+1));
                        reverseResult.add(allPres.get(p+2));
                        reverseResult.add(allPres.get(p+4));
                    }
                    else
                    {
                        result.add(allPres.get(p+4));
                        result.add(allPres.get(p+1));
                        result.add(allPres.get(p+3));

                        reverseResult.add(allPres.get(p));
                        reverseResult.add(allPres.get(p+2));
                        reverseResult.add(allPres.get(p+5));
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


    /*public boolean isCovered(NewPredicate pre, Set<NewPredicate> curr, int t1, int t2) {
        NewTuple tuple1 = originalTable.getTuple(t1);
        NewTuple tuple2 = originalTable.getTuple(t2);

        for (NewPredicate p : curr) {
            if (p.check(tuple1,tuple2))
                return false;
        }

        IntegerPair pair = new IntegerPair(t1,t2);

        if (pre.check(tuple1,tuple2)) {
            if (!evidenceSet.containsKey(pair)) {
                evidenceSet.put(pair, buildSATForPair(tuple1,tuple2));
            return true;
        }

        return false;
    }*/

    public void updateCrit(List<NewPredicate> covered1, List<NewPredicate> covered2, int t1, int t2) {
        if (covered1.size() == 1)
            crit.get(covered1.get(0)).add(new IntegerPair(t1, t2));
        if (covered2.size() == 1)
            crit.get(covered2.get(0)).add(new IntegerPair(t2, t1));
    }

    public BooleanPair checkPredicate(NewPredicate p, int t1, int t2, NewTuple tuple1, NewTuple tuple2) {
        IntegerPair pair1 = new IntegerPair(t1,t2);
        IntegerPair pair2 = new IntegerPair(t2,t1);

        boolean firstKnown = false;
        boolean firstResult = false;
        boolean secondKnown = false;
        boolean secondResult = false;

        if (evidenceSet.containsKey(pair1) && evidenceSet.get(pair1).contains(p)) {
            firstKnown = true;
            firstResult = true;
            //System.out.println("already exists for pair 1");
        }
        else if (reverseEvidenceSet.containsKey(pair1) && reverseEvidenceSet.get(pair1).contains(p)) {
            firstKnown = true;
            firstResult = false;
            //System.out.println("already exists for pair 1");
        }

        if (evidenceSet.containsKey(pair2) && evidenceSet.get(pair2).contains(p)) {
            secondKnown = true;
            secondResult = true;
            //System.out.println("already exists for pair 2");
        }
        else if (reverseEvidenceSet.containsKey(pair2) && reverseEvidenceSet.get(pair2).contains(p)) {
            secondKnown = true;
            secondResult = false;
            //System.out.println("already exists for pair 2");
        }

        if (!firstKnown && !secondKnown) {
            numOfChecks++;
            return p.checkBoth(tuple1, tuple2);
        }
        else if (!firstKnown) {
            return new BooleanPair(p.check(tuple1, tuple2), secondResult);
        }
        else if (!secondKnown) {
            return new BooleanPair(firstResult, p.check(tuple2, tuple1));
        }

        return new BooleanPair(firstResult, secondResult);
    }

    public Pair<IntegerPair, Set<NewPredicate>> buildSat(Set<NewPredicate> curr, int t1, int t2) {
        NewTuple tuple1 = originalTable.getTuple(t1);
        NewTuple tuple2 = originalTable.getTuple(t2);

        boolean firstCovered = false;
        boolean secondCovered = false;

        List<NewPredicate> covered1 = new ArrayList<>();
        List<NewPredicate> covered2 = new ArrayList<>();

        List<NewPredicate> notCovered1 = new ArrayList<>();
        List<NewPredicate> notCovered2 = new ArrayList<>();

        for (NewPredicate p : curr) {
            BooleanPair result = checkPredicate(p, t1, t2, tuple1, tuple2);
            if (result.getPair()[0]) {
                firstCovered = true;
                covered1.add(p);
            }
            else {
                notCovered1.add(p);
            }
            if (result.getPair()[1]) {
                secondCovered = true;
                covered2.add(p);
            }
            else {
                notCovered2.add(p);
            }
        }

        IntegerPair firstPair = new IntegerPair(t1,t2);
        IntegerPair secondPair = new IntegerPair(t2,t1);

        if (evidenceSet.containsKey(firstPair)) {
            evidenceSet.get(firstPair).addAll(covered1);
        }
        else {
            evidenceSet.put(firstPair, new HashSet<>(covered1));
        }

        if (reverseEvidenceSet.containsKey(firstPair)) {
            reverseEvidenceSet.get(firstPair).addAll(notCovered1);
        }
        else {
            reverseEvidenceSet.put(firstPair, new HashSet<>(notCovered1));
        }

        if (evidenceSet.containsKey(secondPair)) {
            evidenceSet.get(secondPair).addAll(covered2);
        }
        else {
            evidenceSet.put(secondPair, new HashSet<>(covered2));
        }

        if (reverseEvidenceSet.containsKey(secondPair)) {
            reverseEvidenceSet.get(secondPair).addAll(notCovered2);
        }
        else {
            reverseEvidenceSet.put(secondPair, new HashSet<>(notCovered2));
        }

        if (firstCovered && secondCovered) {
            updateCrit(covered1, covered2, t1, t2);
            return null;
        }
        else if (!firstCovered) {
            IntegerPair indexes = new IntegerPair(t1, t2);
            Set<NewPredicate> newSAT = new HashSet<>();
            if (evidenceSet.containsKey(indexes) && evidenceSet.size() == (allPres.size() / 2))
                newSAT = evidenceSet.get(indexes);
            else {
                Set<NewPredicate> reverseSAT = new HashSet<>();
                buildSATForPair(newSAT, reverseSAT, tuple1, tuple2);
                evidenceSet.put(indexes, newSAT);
                reverseEvidenceSet.put(indexes, reverseSAT);
            }
            return new Pair<>(indexes, newSAT);
        }
        else {
            IntegerPair indexes = new IntegerPair(t2, t1);
            Set<NewPredicate> newSAT = new HashSet<>();
            if (evidenceSet.containsKey(indexes) && evidenceSet.size() == (allPres.size() / 2))
                newSAT = evidenceSet.get(indexes);
            else {
                Set<NewPredicate> reverseSAT = new HashSet<>();
                buildSATForPair(newSAT, reverseSAT, tuple2, tuple1);
                evidenceSet.put(indexes, newSAT);
                reverseEvidenceSet.put(indexes, reverseSAT);
            }
            return new Pair<>(indexes, newSAT);
        }
    }

    private NewPredicate getSymmetricNewPredicate(NewPredicate pre)
    {
        if(pre.getOperator() == 0 || pre.getOperator() == 1)
            return pre;

        for(NewPredicate temp: allPres)
        {
            if(pre.equalExceptOp(temp))
            {
                if(pre.getOperator() == 2 && temp.getOperator() == 4)
                    return temp;
                if(pre.getOperator() == 4 && temp.getOperator() == 2)
                    return temp;
                if(pre.getOperator() == 5 && temp.getOperator() == 3)
                    return temp;
                if(pre.getOperator() == 3 && temp.getOperator() == 5)
                    return temp;
            }
        }

        return null;
    }

    private boolean areSymmetric(NewDenialConstraint dc1, NewDenialConstraint dc2) {
        ArrayList<NewPredicate> pres1 = dc1.getPredicates();
        ArrayList<NewPredicate> pres2 = dc2.getPredicates();

        if(pres1.size() != pres2.size())
            return false;

        for(int j = 0 ; j < pres1.size(); j++)
        {
            NewPredicate pre1 = pres1.get(j);

            NewPredicate symPre1 = this.getSymmetricNewPredicate(pre1);
            if(!pres2.contains(symPre1))
            {
                return false;
            }
        }

        return true;
    }

    private boolean isSymmetricToOtherDC(NewDenialConstraint dc) {
        for (NewDenialConstraint dc2 : totalDCs) {
            if (areSymmetric(dc, dc2))
                return true;
        }

        return false;
    }

    public List<NewPredicate> sortCurrCand(Set<NewPredicate> currCand) {
        List<NewPredicate> result = new ArrayList<>();
        for (NewPredicate p : allPresSorted) {
            if (currCand.contains(p)) {
                result.add(p);
            }
        }

        return result;
    }

    private void generateMHS( Set<NewPredicate> curr, int globalIndex, int localIndex ) {
        if ( globalIndex == numOfTuples ) { // all sets are covered - minimal hitting set
            ArrayList<NewPredicate> reversedMVC = new ArrayList<>();
            for(NewPredicate pre: curr)
            {
                NewPredicate rPre = getReverseNewPredicate(pre);
                reversedMVC.add(rPre);
            }
            NewDenialConstraint currDC = new NewDenialConstraint(2, reversedMVC);

            totalNumDCs = totalNumDCs + 1;

            // check minimality condition
            /*for ( NewPredicate p : curr ) {
                if ( crit.get( p ).size() == 0 ) {
                    return;
                }
            }   */

            /*if(this.linearImplication(currDC) || isSymmetricToOtherDC(currDC))
            {
                return;
            }*/

            //dcs.add(currDC);
            //minimalCoverAccordingtoInterestingess(dcs);

            //avgTimeBetweenResults = avgTimeBetweenResults + (double)((System.currentTimeMillis() - lastResultTime))/(double)1000;
            //lastResultTime = System.currentTimeMillis();

            totalDCs.add( currDC );
            //System.out.println(currDC.toString());
            return;
        }

        ArrayList<NewPredicate> oldCandidates = new ArrayList<>(candidates);

        Pair<IntegerPair, Set<NewPredicate>> nextSet = null;

        //int endLocalIndex = -1;
        //int endGlobalIndex = -1;
        //int startLocalIndex = localIndex;
        //int startGlobalIndex = globalIndex;

        outerloop:
        for (int j = globalIndex; j < numOfTuples; j++) {
            for (int i = localIndex; i < j; i++) {
                nextSet = buildSat(curr, i, j);
                if (nextSet != null) {
                    localIndex = i;
                    globalIndex = j;
                    break outerloop;
                }
                //endLocalIndex = i;
                //endGlobalIndex = j;
            }
            localIndex = 0;
        }


        //updateCritOld( curr, startLocalIndex, startGlobalIndex, endLocalIndex, endGlobalIndex );

        HashMap<NewPredicate, Set<IntegerPair>> critOld = new HashMap<>();
        for (NewPredicate key: crit.keySet()) {
            critOld.put(key, new HashSet<>(crit.get(key)));
        }

        if (nextSet == null) {
            generateMHS(curr, numOfTuples, 0);
            return;
        }

        Set<NewPredicate> currCand = new HashSet<>(); // currCand - all candidates that belong to uncovSet
        for ( NewPredicate pred : nextSet.getValue() ) {
            if ( candidates.contains( pred ) ) {
                currCand.add( pred );
                candidates.remove( pred ); // candidates = candidates \ currCand
            }
        }

        List<NewPredicate> currCandSorted = sortCurrCand(currCand);

        for ( NewPredicate pred : currCandSorted ) { // try to add to the current set each element of CurrCand
            if (curr.size() == 0 && pred.toString().equalsIgnoreCase("t1.MarriedExemp!=t2.MarriedExemp"))
                System.out.println("aa");
            if (curr.size() == 1 && pred.toString().equalsIgnoreCase("t1.Salary<t2.Salary"))
                System.out.println("bb");
            if (curr.size() == 2 && pred.toString().equalsIgnoreCase("t1.SingleExemp<t2.SingleExemp"))
                System.out.println("bb");

            /*Set<IntegerPair> newCovered = new HashSet<>();
            for (int i=localIndex; i<globalIndex; i++) {
                if (isCovered(pred, curr, i, globalIndex)) {
                    newCovered.add(new Pair<>(i, globalIndex));
                }
                if (isCovered(pred, curr, globalIndex, i)) {
                    newCovered.add(new Pair<>(globalIndex, i));
                }
            }*/

            updateCritNew( pred, curr, nextSet.getKey() );

            boolean minimalityCheck = true; // check minimality condition
            for ( NewPredicate p : curr ) {
                if ( crit.get( p ).size() == 0 ) {
                    minimalityCheck = false;
                    break;
                }
            }

            if ( minimalityCheck ) {
                Set<NewPredicate> candPredToRemove = getNewPredicatesSameAtt( pred );
                Set<NewPredicate> predToRemove = new HashSet<>();
                for (NewPredicate p : candPredToRemove) {
                    if (candidates.contains(p)) {
                        predToRemove.add(p);
                    }
                }
                candidates.removeAll(predToRemove);
                curr.add( pred ); // recursive call with the new set containing the current predicate
                generateMHS( curr, globalIndex, localIndex );
                candidates.addAll(predToRemove);
                curr.remove( pred );
                candidates.add(pred);
            }

            crit = new HashMap<>();
            for (NewPredicate key: critOld.keySet()) {
                crit.put(key, new HashSet<>(critOld.get(key)));
            }
        }

        candidates = new ArrayList<>(oldCandidates);
    }

    public void sortPredicates() {
        ArrayList<NewPredicate> remaining = new ArrayList<>(allPres);
        ArrayList<NewPredicate> toRemove = new ArrayList<>();
        Set<NewPredicate> covered = new HashSet<>();

        for (int i=1; i<numOfTuples; i++) {
            for (int j=0; j<i; j++) {
                NewTuple tuple1 = originalTable.getTuple(j);
                NewTuple tuple2 = originalTable.getTuple(i);

                for (NewPredicate p : remaining) {
                    if (!p.check(tuple1, tuple2) && !covered.contains(p)) {
                        allPresSorted.add(p);
                        toRemove.add(p);
                        covered.add(p);
                    }
                }

                remaining.removeAll(toRemove);
                toRemove = new ArrayList<>();
                if (remaining.isEmpty())
                    return;

                for (NewPredicate p : allPres) {
                    if (!p.check(tuple2, tuple1) && !covered.contains(p)) {
                        allPresSorted.add(p);
                        toRemove.add(p);
                        covered.add(p);
                    }
                }

                remaining.removeAll(toRemove);
                toRemove = new ArrayList<>();
                if (remaining.isEmpty())
                    return;
            }
        }

        allPresSorted.addAll(remaining);
    }

    public List<NewDenialConstraint> generateDCs () {
        System.out.println("Sorting...");
        sortPredicates();

        System.out.println("Generating DCs...");
        candidates = new ArrayList<>(allPres);
        generateMHS( new HashSet<>(), 1 , 0);

        System.out.println("total number of DCs is " + totalNumDCs);
        System.out.println("total number of generated evidence sets is " + evidenceSetNum);

        System.out.println((System.currentTimeMillis() - startingTime )/ 1000 +
                "Done searching for DCs: " + evidence.size());

        System.out.println("Num of checks: " + numOfChecks);
        
        return totalDCs;



        //avgTimeBetweenResults = avgTimeBetweenResults / (double)dcs.size();
        //System.out.println("Time between results: " + avgTimeBetweenResults);
    }


    /**
     * This is the function where you can add all predicates that you are interested in
     * The set of operators you are interested in
     * Allowing comparison within a tuple?
     * Allowing comparison cross columns?
     */
    private void findAllNewPredicates()
    {

        numOfCols = originalTable.getNumCols();
        int index_predicate = 0;
        for(int col = 1; col <= numOfCols; col++)
        { // EQ - 0, IQ - 1, GT - 2, LTE - 3, LT - 4, GTE - 5
            NewPredicate pre = new NewPredicate(originalTable,0,1,2,col,col);
            pre.setIndex(index_predicate);
            index_predicate = index_predicate + 1;
            allPres.add(pre);
            pre = new NewPredicate(originalTable,1,1,2,col,col);
            pre.setIndex(index_predicate);
            index_predicate = index_predicate + 1;
            allPres.add(pre);


            int type = originalTable.getColumnMapping().positionToType(col);
            if(type == 0 || type == 1)
            {
                pre = new NewPredicate(originalTable,2,1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);
                pre = new NewPredicate(originalTable,3,1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);

                pre = new NewPredicate(originalTable,4,1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);

                pre = new NewPredicate(originalTable,5,1,2,col,col);
                pre.setIndex(index_predicate);
                index_predicate = index_predicate + 1;
                allPres.add(pre);
            }

        }

        dbPro = new DBProfiler(originalTable);

        if(Config.enableCrossColumn)
        {
            Set<String> eqCompa = dbPro.getEquComCols2();
            Set<String> orderCompa = dbPro.getOrderComCols2();

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
                    NewPredicate pre = new NewPredicate(originalTable,0,row1,row2,col1,col2);
                    allPres.add(pre);
                    //singleTupleVarPre.add(pre);
                    pre = new NewPredicate(originalTable,1,row1,row2,col1,col2);
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
                    NewPredicate pre = new NewPredicate(originalTable,2,row1,row2,col1,col2);
                    allPres.add(pre);
                    pre = new NewPredicate(originalTable,3,row1,row2,col1,col2);
                    allPres.add(pre);
                    pre = new NewPredicate(originalTable,4,row1,row2,col1,col2);
                    allPres.add(pre);
                    pre = new NewPredicate(originalTable,5,row1,row2,col1,col2);
                    allPres.add(pre);
                }
            }



        }
        System.out.println("Number of variable predicates: " + allPres.size());
        //ArrayList<NewPredicate> consPres = dbPro.findkfrequentconstantpredicate();
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

        final ArrayList<Map<Set<NewPredicate>, Long>> tempPairInfo
                = new ArrayList<Map<Set<NewPredicate>, Long>>();


        System.out.println("We are going to use " + numThreads + " threads");
        final int numRows = originalTable.getNumRows();
        int chunk = numRows / numThreads;

        for(int k = 0 ; k < numThreads; k++)
        {
            final int kTemp = k;
            final int startk = k * chunk;
            final int finalk = (k==numThreads-1)? numRows:(k+1)*chunk;
            //tempInfo.add(new HashMap<Set<NewPredicate>,Long>());

            Map<Set<NewPredicate>, Long> tempPairInfok = new HashMap<Set<NewPredicate>, Long>();
            tempPairInfo.add(tempPairInfok);
            Thread thread = new Thread(new Runnable()
            {
                public void run() {
                    Map<Set<NewPredicate>, Long> tempPairInfok = tempPairInfo.get(kTemp);

                    int numCols = originalTable.getNumCols();
                    for(int t1 = startk ; t1 < finalk; t1++)
                        for(int t2 = 0 ; t2 < numRows; t2++)
                        {

                            NewTuple tuple1 = originalTable.getTuple(t1);
                            NewTuple tuple2 = originalTable.getTuple(t2);

                            if(tuple1.tid == tuple2.tid)
                                continue;

                            Set<NewPredicate> cur = new HashSet<NewPredicate>();

                            //The naive way

                            int p = 0;

                            if(Config.qua == 1)
                            {
                                for(int col = 1; col <= numCols; col++)
                                {

                                    NewPredicate pre = allPres.get(p);

                                    int type = originalTable.getColumnMapping().positionToType(col);

                                    if(type == 2)
                                    {

                                        int rel = 0; // 0- EQ, -1 -> IQ, 1- > , 2- <
                                        if(dbPro.equalOrNot(col-1, tuple1.getCell(col-1).getStringValue(), tuple2.getCell(col-1).getStringValue()))
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

                                NewPredicate pre = allPres.get(p);
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
            Map<Set<NewPredicate>, Long> tempPairInfok = tempPairInfo.get(k);
            for(Set<NewPredicate> cur: tempPairInfok.keySet())
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
        for (Set<NewPredicate> k : evidence.keySet()) {
            tempind = predicates_to_indexes(k);
            evidence_ind.add(tempind);
        }

        for (Set<Integer> intset : evidence_ind)
            System.out.println(intset.toString() + ',');*/
    }


    private Set<Integer> predicates_to_indexes(Set<NewPredicate> preset) {
        Set<Integer> indexes = new HashSet<>();
        for (NewPredicate p : preset) {
            indexes.add(p.getIndex());
        }
        return indexes;
    }




    /**
     * Get the reverse predicate of all predicates
     * @param pre
     * @return
     */
    public NewPredicate getReverseNewPredicate(NewPredicate pre)
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

    private Map<NewPredicate,Set<NewPredicate>> impliedMap = new HashMap<NewPredicate,Set<NewPredicate>>();
    /**
     * Get the set of predicates implied by the passed-in predicate, does not include the pre itself
     * @param pre
     * @return
     */
    public Set<NewPredicate> getImpliedNewPredicates(NewPredicate pre)
    {
        if(pre == null)
            return new HashSet<NewPredicate>();

        if(!impliedMap.containsKey(pre))
        {
            Set<NewPredicate> result = new HashSet<NewPredicate>();
            for(NewPredicate temp: allPres)
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
            for(NewDenialConstraint dc: totalDCs)
            {
                out.println(dc.interestingness + ":" + dc.toString());
            }
            out.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public Set<Set<NewPredicate>> discover()
    {
        System.out.println("Discovering...");

        //Do not discover DCs containing done
        Set<NewPredicate> done = new HashSet<>();
        for(NewPredicate curPre: allPres)
        {
            if(curPre.getOperator() == 5 || curPre.getOperator() == 3)
            {
                if(!curPre.isSecondCons())
                    done.add(curPre);
            }

        }

        Set<Set<NewPredicate>> allMVC = new HashSet<Set<NewPredicate>>();
        if(evidence.size()!=0)
            getAllMVCDFS(allMVC,evidence,done,totalDCs);
        else
            allMVC.add(new HashSet<NewPredicate>());

        for(Set<NewPredicate> mvc: allMVC)
        {
            //Reverse the predicate
            ArrayList<NewPredicate> reversedMVC = new ArrayList<NewPredicate>();
            for(NewPredicate pre: mvc)
            {
                NewPredicate aaa = getReverseNewPredicate(pre);
                reversedMVC.add(aaa);
            }
            NewDenialConstraint dc = new NewDenialConstraint(2, reversedMVC);
            totalDCs.add(dc);
        }


        System.out.println((System.currentTimeMillis() - startingTime)/1000 + ":" +
                "Total number of DCs: " + totalDCs.size());
        System.out.println((System.currentTimeMillis() - startingTime)/1000 + ":" +
                "*************Done Constraints Discovery******************");

        return allMVC;
    }
    private void getAllMVCDFS(Set<Set<NewPredicate>> allMVCs, Map<Set<NewPredicate>,Long> toBeCovered,Set<NewPredicate> myDone,
                              ArrayList<NewDenialConstraint> dcs)
    {
        //Get the ordering of predicate, based on their coverage
        Set<NewPredicate> curMVC = new HashSet<NewPredicate>();
        ArrayList<NewPredicate> candidates = new ArrayList<NewPredicate>();
        for(NewPredicate temp: allPres)
        {
            for(Set<NewPredicate> key: toBeCovered.keySet())
                if(key.contains(temp))
                {
                    NewPredicate reverse = getReverseNewPredicate(temp);
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
        ArrayList<NewPredicate> ordering = getOrdering2(toBeCovered,candidates,curMVC);
        System.out.println("Generating hitting sets...");
        getAllMVCDFSRecursive(allMVCs,toBeCovered,ordering,curMVC,myDone,dcs,null,toBeCovered);
        System.out.println("Done generating hitting sets...");
    }
    private void getAllMVCDFSRecursive(Set<Set<NewPredicate>> allMVCs,Map<Set<NewPredicate>,Long> toBeCovered,
                                       ArrayList<NewPredicate> ordering,Set<NewPredicate> curMVC,Set<NewPredicate> myDone, ArrayList<NewDenialConstraint> dcs,NewPredicate lastMVC,
                                       Map<Set<NewPredicate>,Long> OriginalToBeCovered)
    {


        //If the size of the branch gets too long, return

        /*if(Config.dfsLevel != -1)
        {
            if(curMVC.size() + 1 > Config.dfsLevel)
                return;
        }*/



        //subset pruning
        boolean tag = false;
        for(Set<NewPredicate> tempMVC: allMVCs)
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
            Set<NewPredicate> newMVC = new HashSet<NewPredicate>(curMVC);
            allMVCs.add(newMVC);
            System.out.println((System.currentTimeMillis() - startingTime) / 1000 + ":" +
                    "New Hitting Set: " + newMVC.toString());
            return;
        }

        ArrayList<NewPredicate> remaining = new ArrayList<NewPredicate>(ordering);
        for(int i = 0 ; i < ordering.size(); i++)
        {

            NewPredicate curPre = ordering.get(i);
            remaining.remove(curPre);


            Map<Set<NewPredicate>,Long> nextToBeCovered = new HashMap<Set<NewPredicate>,Long>();
            for(Set<NewPredicate> temp2: toBeCovered.keySet())
            {
                if(!temp2.contains(curPre))
                {
                    nextToBeCovered.put(temp2, toBeCovered.get(temp2));
                }
            }

            curMVC.add(curPre);
            ArrayList<NewPredicate> nextOrdering = getOrdering2(nextToBeCovered,remaining, curMVC);
            getAllMVCDFSRecursive(allMVCs,nextToBeCovered,nextOrdering,curMVC,myDone,dcs,curPre,OriginalToBeCovered);
            curMVC.remove(curPre);
        }
    }

    private ArrayList<NewPredicate> getOrdering2(Map<Set<NewPredicate>,Long> toBeCovered,ArrayList<NewPredicate> candidate, Set<NewPredicate> curMVC)
    {
        ArrayList<NewPredicate> result = new ArrayList<NewPredicate>();
        for(NewPredicate pre: candidate)
        {
            //Get rid of trivial DC at this point
            boolean tagTrivial = false;
            NewPredicate reverse = getReverseNewPredicate(pre);
            Set<NewPredicate> revImplied = getImpliedNewPredicates(reverse);
            for(NewPredicate temp: curMVC)
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
            for(Set<NewPredicate> temp: toBeCovered.keySet())
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
    private boolean minimalityTest(Set<NewPredicate> curMVC, Map<Set<NewPredicate>,Long> OriginalToBeCovered)
    {
        if(Config.noiseTolerance != 0)
        {
            Set<NewPredicate> tempMVC = new HashSet<NewPredicate>(curMVC);
            for(NewPredicate leftOut: curMVC)
            {
                tempMVC.remove(leftOut);
                //Test whether temMVC is a cover or not
                long numVios = 0;
                for(Set<NewPredicate> key: OriginalToBeCovered.keySet())
                {
                    boolean thisKeyCovered = false;
                    for(NewPredicate pre: tempMVC)
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
            Set<NewPredicate> tempMVC = new HashSet<NewPredicate>(curMVC);
            for(NewPredicate leftOut: curMVC)
            {
                tempMVC.remove(leftOut);
                //Test whether temMVC is a cover or not
                boolean isCover = true;
                for(Set<NewPredicate> key: OriginalToBeCovered.keySet())
                {
                    boolean thisKeyCovered = false;
                    for(NewPredicate pre: tempMVC)
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
    public long getMaxVios(Set<NewPredicate> pres)
    {
        boolean twoTuple = false;
        for(NewPredicate temp: pres)
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
