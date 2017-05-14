// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package main

import (
	"extip"
	"flag"
	"fmt"
	"hyperviser"
)

// Command line arguments.
var (
	verbose       bool
	isLeader      bool
	testType      string
	password      string
	testDirPath   string
	leaderLogFile string
	testLogFile   string
)

const (
	tHelp    = "Name of the test to run"
	pHelp    = "Test authentication password"
	lHelp    = "Is this node a test leader?"
	dirHelp  = "Log file directory"
	lLogHelp = "Name of leader log file"
	tLogHelp = "Name of test log file"
	vHelp    = "More expressive output"
)

func init() {
	flag.StringVar(&testType, "test", "", tHelp)
	flag.StringVar(&password, "password", "", pHelp)
	flag.BoolVar(&isLeader, "leader", false, lHelp)
	flag.StringVar(&testDirPath, "testdir", "./", dirHelp)
	flag.StringVar(&leaderLogFile, "leaderfile", "", lLogHelp)
	flag.StringVar(&testLogFile, "testfile", "", tLogHelp)
	flag.BoolVar(&verbose, "verbose", false, vHelp)

	flag.StringVar(&testType, "t", "", tHelp)
	flag.StringVar(&password, "p", "", pHelp)
	flag.BoolVar(&isLeader, "l", false, lHelp)
	flag.StringVar(&testDirPath, "d", "./", dirHelp)
	flag.StringVar(&leaderLogFile, "lf", "", lLogHelp)
	flag.StringVar(&testLogFile, "tf", "", tLogHelp)
	flag.BoolVar(&verbose, "v", false, vHelp)
}

func printFlagErrorMessage() {
	fmt.Println("Program for starting and manipulating a hyperviser")
	fmt.Println("Usage:")
	fmt.Printf("\t-test, -t\n\t\t%s\n", tHelp)
	fmt.Printf("\t-password, -p\n\t\t%s\n", pHelp)
	fmt.Printf("\t-leader, -l\n\t\t%s\n", lHelp)
	fmt.Printf("\t-testDir, -d\n\t\t%s\n", dirHelp)
	fmt.Printf("\t-leaderfile, -lf\n\t\t%s\n", lLogHelp)
	fmt.Printf("\t-testfile, -tl\n\t\t%s\n", tLogHelp)
	fmt.Printf("\t-verbose, -v\n\t\t%s\n", vHelp)
}

func main() {
	flag.Parse()

	if verbose {
		fmt.Println("testname:", testType)
		fmt.Println("password:", password)
		fmt.Println("leader:", isLeader)
		fmt.Println("Directory path:", testDirPath)
		fmt.Println("Leader log file:", leaderLogFile)
		fmt.Println("test log file:", testLogFile)
	}

	// TODO: More robust checks?
	if testType == "" || password == "" || testLogFile == "" {
		fmt.Println("Invalid usage")
		printFlagErrorMessage()
		return
	}

	ip, err := extip.ExternalIP()
	if err != nil {
		fmt.Printf("Get IP address failed: %s\n", err.Error())
		return
	}

	hv, err := hyperviser.Make(ip, password, testDirPath)

	if err != nil {
		fmt.Printf("Hyperviser initialization failed: %s\n", err.Error())
		return
	}

	if isLeader {
		fmt.Println("Starting Leader ...")
		err := hv.StartLeader(hyperviser.TestType(testType), leaderLogFile, testLogFile)
		if err != nil {
			fmt.Printf("Test failed: %s\n", err.Error())
		} else {
			fmt.Println("Success!")
		}
	} else {
		fmt.Println("Started Follower...")
		// if not leader, wait forever
		ch := make(chan bool)
		<-ch
	}
}
