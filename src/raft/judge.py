import subprocess
import argparse
import time

parser = argparse.ArgumentParser(description='Run go test multiple times.')
parser.add_argument('--count', type=int, required=True, help='Number of times to run the command')
parser.add_argument('--pattern', type=str, required=True, help='Pattern to run')
parser.add_argument('--verbose', action='store_true', help='Print verbose output')

args = parser.parse_args()
repeat_count = args.count
pattern = args.pattern
verbose = args.verbose

command = ['go', 'test', '-run', pattern, '-race', '-v']

flag = True
start = time.time()
for i in range(repeat_count):
    # Run the command and capture the output
    print(f"== Round {i+1} start ==")
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
    print(f"== Round {i+1} finished {round_end - round_start:.2f} s ==\n")

end = time.time()
print(f"Total time: {end - start:.2f} s")
if flag:
    print(f"All Test passed")