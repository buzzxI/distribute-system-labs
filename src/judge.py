import subprocess
import argparse
import time
import re
import logging
import asyncio
import telegram
from enum import Enum

class Target(Enum):
    # LAB2 = 'lab2'
    LAB3 = 'lab3'
    LAB4 = 'lab4'

def load_testnames(filename):
    with open(filename) as f:
        return [line.strip() for line in f]
    
def run_command(command, cwd=None):
    flag = True

    for i in range(repeat_count):
        # Run the command and capture the output
        logging.info(f"Round {i+1} start")
        round_start = time.time() 

        if verbose:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=cwd)

            for line in process.stdout:
                logging.info(line.rstrip())
            process.wait()
            if process.returncode != 0:
                logging.error(f"Round {i + 1} Test failed")
                flag = False
                break
        else :
            result = subprocess.run(command, capture_output=True, text=True, cwd=cwd)
            if result.returncode != 0:
                logging.info(result.stdout)
                logging.info(result.stderr)
                logging.error(f"Round {i + 1} Test failed")
                flag = False
                break   
        
        round_end = time.time()
        logging.info(f"Round {i+1} finished, consume {round_end - round_start:.2f} s")

    return flag

def report_result(failTest):
    async def error_report():
        bot = telegram.Bot(telegram_token)
        content = f'judge {target.value} fail, round {repeat_count}, pattern {pattern}, fail test {failTest}, total time consume {judge_duration:.2f} s'
        await bot.send_message(chat_id=telegram_chatid, text=content)
        await bot.send_document(chat_id=telegram_chatid, document='./log.txt')

    async def success_report():
        bot = telegram.Bot(telegram_token)
        content = f'judge {target.value} success, round {repeat_count}, pattern {pattern}, total time consume {judge_duration:.2f} s'
        await bot.send_message(chat_id=telegram_chatid, text=content)

    if failTest:
        asyncio.run(error_report())
    else:
        asyncio.run(success_report())

parser = argparse.ArgumentParser(description='Run go test multiple times.')
parser.add_argument('--count', type=int, required=True, help='Number of times to run the command')
parser.add_argument('--target', type=Target, choices=list(Target), required=True, help='Lab number')
parser.add_argument('--pattern', type=str, required=True, help='Pattern to run (regexp)')
parser.add_argument('--verbose', action='store_true', help='Print verbose output')
parser.add_argument('--race', action='store_true', help='Run with race detector')
parser.add_argument('--telegram-token', type=str, required=False, help='telegram token is used to report log to telegram bot')
parser.add_argument('--telegram-chatid', type=str, required=False, help='telegram chat id is used to report log to user')

args = parser.parse_args()
repeat_count = args.count
target = args.target
pattern = args.pattern
verbose = args.verbose
race = args.race
telegram_token = args.telegram_token
telegram_chatid = args.telegram_chatid

working_dir = '.'

if target == Target.LAB3:
    working_dir = f'{working_dir}/raft'
elif target == Target.LAB4:
    working_dir = f'{working_dir}/kvraft'

testlist = load_testnames(f'{working_dir}/testlist.txt')

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        # clear old log file
        logging.FileHandler("log.txt", mode='w'),
        logging.StreamHandler()]
)

# suppress PTB http request output
logging.getLogger('httpx').setLevel(logging.ERROR)
logging.getLogger('httpcore').setLevel(logging.ERROR)

testsToRun = [test for test in testlist if re.search(pattern, test)]

failTest = None

judge_start = time.time()
for test in testsToRun:
    command = ['go', 'test', '-run', test, '-v']
    speedtest = False
    if "Speed" in test:
        speedtest = True
    # speed test should not run with race flag
    if race and not speedtest:
        command.append("-race")
    start = time.time()
    logging.info(f"Running {test}")
    rst = run_command(command, working_dir)
    end = time.time()
    logging.info(f"Test {test} consume {end - start:.2f} s")
    if not rst:
        logging.error(f"Test {test} failed\n")
        failTest = test
        break
    else:
        logging.info(f"Test {test} passed\n")

judge_end = time.time()
judge_duration = judge_end - judge_start
logging.info(f"Total consume: {judge_duration:.2f} s")
if not failTest:
    logging.info(f"All Test passed")

if telegram_token and telegram_chatid:
    report_result(failTest)