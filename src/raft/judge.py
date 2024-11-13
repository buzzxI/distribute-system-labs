import subprocess
import argparse
import time
import sys
import re

def load_testnames(filename):
    with open(filename) as f:
        return [line.strip() for line in f]
    
def run_command(command):
    flag = True

    for i in range(repeat_count):
        # Run the command and capture the output
        print(f"== Round {i+1} start ==")
        sys.stdout.flush()
        round_start = time.time() 

        if verbose:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

            for line in process.stdout:
                print(line, end='')
            process.wait()
            if process.returncode != 0:
                print(f"Round {i + 1} Test failed\n")
                flag = False
                break
        else :
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode != 0:
                print(result.stdout)  # Print standard output
                print(result.stderr)   # Print standard error
                print(f"Round {i + 1} Test failed\n")
                flag = False
                break   
        
        round_end = time.time()
        print(f"== Round {i+1} finished, consume {round_end - round_start:.2f} s ==\n")
        sys.stdout.flush()

    return flag

parser = argparse.ArgumentParser(description='Run go test multiple times.')
parser.add_argument('--count', type=int, required=True, help='Number of times to run the command')
parser.add_argument('--pattern', type=str, required=True, help='Pattern to run (regexp)')
parser.add_argument('--verbose', action='store_true', help='Print verbose output')
parser.add_argument('--testlist', type=str, default='./testlist.txt', help='File containing the list of tests to run')
parser.add_argument('--race', action='store_true', help='Run with race detector')

args = parser.parse_args()
repeat_count = args.count
pattern = args.pattern
verbose = args.verbose
testlistPath = args.testlist
race = args.race

testlist = load_testnames(testlistPath)

testsToRun = [test for test in testlist if re.search(pattern, test)]

flag = True

start = time.time()
for test in testsToRun:
    command = ['go', 'test', '-run', test, '-v']
    if race:
        command.append("-race")
    start = time.time()
    print(f"Running {test}")
    rst = run_command(command)
    end = time.time()
    print(f"Test {test} consume {end - start:.2f} s")
    if not rst:
        print(f"Test {test} failed\n")
        flag = False
        break
    else:
        print(f"Test {test} passed\n")
        sys.stdout.flush()

end = time.time()
print(f"Total consume: {end - start:.2f} s")
if flag:
    print(f"All Test passed")