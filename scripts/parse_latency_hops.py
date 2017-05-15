#Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

import os
from os.path import join


# paths of input files
inPaths = ["../data/distinct-datacenters/serv0/out10", "../data/distinct-datacenters/serv1/out10"]
# path for output file
outPath = "out10_aggregate.csv"
# columns/indices to select . Zero-indexed
indices = [3, 7, 8, 9]

# write the filtered contents of fIn to fOut
def writeToOutFile(num, fOut, fIn):
    for idx, line in enumerate(fIn):
        num += 1
        inList = line.split()
        outList = [str(num)] + [inList[i] for i in indices]
        fOut.write(",".join(outList) + "\n")
    return num
# write files form the inpaths to the outpath
def aggregateFiles(outPath, inPaths):
    fOut = open(outPath,'w')
    num = 0
    for inPath in inPaths:
        fIn = open(inPath)
        num = writeToOutFile(num, fOut,fIn)
    fOut.close


# main function
def main():
    distinct_inPaths = {}
    shared_inPaths = {}
    recursive_inPaths = {}
    outPath = {}

    for root, dirs, files in os.walk('../data'):
        if "serv" in root:
            inPaths = {}
            if "distinct" in root:
                inPaths = distinct_inPaths
            elif "shared" in root:
                inPaths = shared_inPaths
            elif "recursive" in root:
                inPaths = recursive_inPaths

            for f_in in files:
                if f_in not in inPaths:
                    inPaths[f_in] = [join(root,f_in)]
                else:
                    inPaths[f_in].append(join(root,f_in))

    for testName, paths in distinct_inPaths.items():
        print "Aggregating" + testName
        aggregateFiles("../data/" + "distinct-lookup-" + testName + ".csv", paths)

    for testName, paths in shared_inPaths.items():
        print "Aggregating" + testName
        aggregateFiles("../data/" + "shared-lookup-" + testName + ".csv", paths)

    for testName, paths in recursive_inPaths.items():
        print "Aggregating" + testName
        aggregateFiles("../data/" + "recursive-lookup-" + testName + ".csv", paths)



if __name__ == "__main__":
    main()
