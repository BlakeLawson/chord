// Blake Lawson (blawson@princeton.edu) and Oluwatosin Adewale (oadewale@princeton.edu)

package main

import (
	"flag"
	"fmt"
	"hyperviser"
)

var testType string
var password string
var isLeader bool
var testDirPath string
var leaderLogFile string
var testLogFile string

const (
	ip    string = "127.0.0.1"
	None  string = "none"
	DEBUG bool   = true
)

// initialize flags
func init() {
	flag.StringVar(&testType, "type", None, "Name of the test you want to run.")
	flag.StringVar(&password, "password", None, "Password to start test.")
	flag.BoolVar(&isLeader, "leader", false, "Is this node a test leader?")
	flag.StringVar(&testDirPath, "testdir", None, "Test Directory.")
	flag.StringVar(&leaderLogFile, "leaderfile", None, "Name of file for leader to log to.")
	flag.StringVar(&testLogFile, "testfile", None, "Name of file to log test results to.")

	flag.StringVar(&testType, "t", None, "Name of the test you want to run.")
	flag.StringVar(&password, "p", None, "Password to start test.")
	flag.BoolVar(&isLeader, "l", false, "Is this node a test leader?")
	flag.StringVar(&testDirPath, "d", None, "Test Directory.")
	flag.StringVar(&leaderLogFile, "lf", None, "Name of file for leader to log to.")
	flag.StringVar(&testLogFile, "tf", None, "Name of file to log test results to.")
}

func printFlagErrorMessage() {
	fmt.Println("\tProgram for starting and manipulating a hyperviser")
	fmt.Println("\tFlags:")
	fmt.Printf("\t%s\t %s:\n\t%s\n", flag.Lookup("t").Name, flag.Lookup("type").Name,
		flag.Lookup("t").Usage)
	fmt.Printf("\t%s\t %s:\n\t%s\n", flag.Lookup("p").Name, flag.Lookup("password").Name,
		flag.Lookup("p").Usage)
	fmt.Printf("\t%s\t %s:\n\t%s\n", flag.Lookup("l").Name, flag.Lookup("leader").Name,
		flag.Lookup("l").Usage)
	fmt.Printf("\t%s\t %s:\n\t%s\n", flag.Lookup("d").Name, flag.Lookup("testdir").Name,
		flag.Lookup("d").Usage)
	fmt.Printf("\t%s\t %s:\n\t%s\n", flag.Lookup("lf").Name, flag.Lookup("leaderfile").Name,
		flag.Lookup("lf").Usage)
	fmt.Printf("\t%s\t %s:\n\t%s\n", flag.Lookup("tf").Name, flag.Lookup("testfile").Name,
		flag.Lookup("tf").Usage)
}

func main() {
	flag.Parse()

	if DEBUG {
		fmt.Println("testname:", testType)
		fmt.Println("password:", password)
		fmt.Println("leader:", isLeader)
		fmt.Println("Directory path:", testDirPath)
		fmt.Println("Leader log file:", leaderLogFile)
		fmt.Println("test log file:", testLogFile)
	}

	if testType == None || password == None || testLogFile == None {
		printFlagErrorMessage()
		return
	}

	if isLeader && testDirPath == None {
		printFlagErrorMessage()
		return
	}

	// TODO: more robust checks?
	hv, err := hyperviser.Make(ip, password, testDirPath)

	if err != nil {
		fmt.Printf("Error: err: %s", err.Error())
		return
	}
	// TODO
	if isLeader {
		fmt.Println("\n\nStarting Leader...")
		err := hv.StartLeader(hyperviser.TestType(testType), leaderLogFile, testLogFile)
		if err != nil {
			fmt.Printf("Error: err: %s", err.Error())
			return
		}
	} else {
		// if not leader, wait forever
		fmt.Println("\n\nStarted Follower...")
		for {

		}
	}
	return
}
