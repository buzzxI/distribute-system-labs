import subprocess
import sys

def run_script_multiple_times(script_path, num_trials, output_file):
    with open(output_file, 'w') as f:
        for i in range(1, num_trials + 1):
            print(f"Running trial {i}...")
            f.write(f"Running trial {i}...\n")
            try:
                result = subprocess.run(['zsh', '-c', script_path], capture_output=True, text=True, timeout=900)
                f.write(result.stdout)
                f.write(result.stderr)
                if result.returncode != 0:
                    f.write(f"*** FAILED TESTS IN TRIAL {i}\n")
                    print(f"*** FAILED TESTS IN TRIAL {i}")
                    break
            except subprocess.TimeoutExpired:
                f.write(f"*** TIMEOUT EXPIRED IN TRIAL {i}\n")
                print(f"*** TIMEOUT EXPIRED IN TRIAL {i}")
                break
        else:
            f.write(f"*** PASSED ALL {num_trials} TESTING TRIALS\n")
            print(f"*** PASSED ALL {num_trials} TESTING TRIALS")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python run_test_mr_multiple_times.py <script_path> <num_trials>")
        sys.exit(1)

    script_path = sys.argv[1]
    num_trials = int(sys.argv[2])
    output_file = "test_mr_output.txt"

    run_script_multiple_times(script_path, num_trials, output_file)