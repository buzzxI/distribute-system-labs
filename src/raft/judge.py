import subprocess
import argparse

parser = argparse.ArgumentParser(description='Run go test multiple times.')
parser.add_argument('--count', type=int, required=True, help='Number of times to run the command')
parser.add_argument('--pattern', type=str, required=True, help='Pattern to run')

args = parser.parse_args()
repeat_count = args.count
pattern = args.pattern

command = ['go', 'test', '-run', pattern, '-race', '-v']

for i in range(repeat_count):
    # Run the command and capture the output
    print(f"Round {i+1}")
    result = subprocess.run(command, capture_output=True, text=True)

    # Check if the test failed
    if result.returncode != 0:
        print("Test failed\n")
        print(result.stdout)  # Print standard output
        print(result.stderr)   # Print standard error
        break
    else:
        print("Test passed\n")